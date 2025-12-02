from __future__ import annotations

import os
from typing import Dict

from app.db.mongo import get_db

COLL_DECLAREPT = os.getenv("COLL_DECLAREPT", "declare_pt_events")
COLL_CONSUMIRVASOT = os.getenv("COLL_CONSUMIRVASOT", "consume_vasot_events")


async def _sum_skus_by_work_order(db, collection_name: str, work_order: str) -> Dict[str, float]:
    """
    Suma las cantidades por SKU para una work_order en una colección dada.
    Solo considera documentos con status SUCCESS.
    """
    col = db[collection_name]
    pipeline = [
        {"$match": {"work_order": work_order, "status": "SUCCESS"}},
        {"$project": {"skus": {"$objectToArray": "$skus"}}},
        {"$unwind": "$skus"},
        {"$group": {"_id": "$skus.k", "total": {"$sum": "$skus.v"}}},
    ]
    results = await col.aggregate(pipeline).to_list(length=None)
    return {str(doc["_id"]): float(doc["total"]) for doc in results}


async def get_net_skus_by_ot(db=None, ot: int | str = None) -> Dict[str, float]:
    """
    Retorna la diferencia de cantidades por SKU:
    DECLARE_PT (positivo) - CONSUMIR_VASOT (negativo) para la OT indicada.
    """
    if ot is None:
        raise ValueError("OT es requerida")
    try:
        ot_int = int(ot)
    except (TypeError, ValueError) as exc:
        raise ValueError("OT debe ser un entero") from exc

    work_order = f"OT-{ot_int}"
    database = db if db is not None else get_db()

    declare_totals = await _sum_skus_by_work_order(database, COLL_DECLAREPT, work_order)
    consume_totals = await _sum_skus_by_work_order(database, COLL_CONSUMIRVASOT, work_order)

    # Partimos de los valores declarados (DECLARE_PT) y restamos consumos
    net: Dict[str, float] = {sku: float(qty) for sku, qty in declare_totals.items()}
    for sku, qty in consume_totals.items():
        if sku in net:
            net[sku] = net.get(sku, 0.0) - float(qty)

    # Limpia ceros exactos
    net = {sku: qty for sku, qty in net.items() if qty != 0}

    return net
