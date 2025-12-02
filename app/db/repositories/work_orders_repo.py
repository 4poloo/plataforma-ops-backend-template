from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Sequence, Tuple

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING, ReturnDocument

from app.db.mongo import get_db

_COLLECTION_NAME = "work_orders"


async def get_collection(db: Optional[AsyncIOMotorDatabase] = None) -> AsyncIOMotorCollection:
    database = db if db is not None else get_db()
    return database[_COLLECTION_NAME]


async def ensure_indexes(db: Optional[AsyncIOMotorDatabase] = None) -> None:
    col = await get_collection(db)
    await col.create_index("OT", unique=True, name="uq_work_orders_ot")


async def find_by_id(
    _id: ObjectId,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Dict[str, Any]]:
    col = await get_collection(db)
    return await col.find_one({"_id": _id})


async def find_by_ot(
    ot: int,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Dict[str, Any]]:
    col = await get_collection(db)
    return await col.find_one({"OT": int(ot)})


async def find_last_ot(
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Dict[str, Any]]:
    col = await get_collection(db)
    return await col.find_one(sort=[("OT", DESCENDING)])


async def find_last_created(
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Dict[str, Any]]:
    """Retorna la última OT creada priorizando la fecha de creación."""
    col = await get_collection(db)
    return await col.find_one(
        sort=[("audit.createdAt", DESCENDING), ("OT", DESCENDING)],
        projection={"OT": 1, "audit.createdAt": 1},
    )


async def insert_work_order(
    work_order: Dict[str, Any],
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Dict[str, Any]:
    col = await get_collection(db)
    res = await col.insert_one(work_order)
    return await col.find_one({"_id": res.inserted_id})


async def list_work_orders(
    *,
    limit: int = 50,
    skip: int = 0,
    filtro: Optional[Dict[str, Any]] = None,
    sort: Optional[Sequence[Tuple[str, int]]] = None,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> list[Dict[str, Any]]:
    col = await get_collection(db)
    query = filtro or {}
    cursor = col.find(query)
    if sort:
        cursor = cursor.sort(list(sort))
    else:
        cursor = cursor.sort("OT", ASCENDING)
    cursor = cursor.skip(int(skip)).limit(int(limit))
    return [doc async for doc in cursor]


async def count_work_orders(
    *,
    filtro: Optional[Dict[str, Any]] = None,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> int:
    col = await get_collection(db)
    return await col.count_documents(filtro or {})


async def update_estado_by_ot(
    ot: int,
    estado: str,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Dict[str, Any]]:
    col = await get_collection(db)
    return await col.find_one_and_update(
        {"OT": int(ot)},
        {
            "$set": {
                "estado": estado,
                "audit.updatedAt": datetime.now(timezone.utc),
            }
        },
        return_document=ReturnDocument.AFTER,
    )


async def close_until_fecha(
    fecha_fin_exclusive: datetime,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> int:
    """
    Marca como CERRADA todas las OT cuyo campo contenido.fecha sea
    menor a 'fecha_fin_exclusive' y no estén ya cerradas.
    Solo se actualiza el campo 'estado'.
    """
    col = await get_collection(db)
    result = await col.update_many(
        {
            "contenido.fecha": {"$lt": fecha_fin_exclusive},
            "estado": {"$ne": "CERRADA"},
        },
        {"$set": {"estado": "CERRADA"}},
    )
    return int(result.modified_count)
