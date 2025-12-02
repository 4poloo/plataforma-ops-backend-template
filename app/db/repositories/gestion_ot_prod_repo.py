from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Sequence, Tuple

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ReturnDocument

from app.db.mongo import get_db

_COLLECTION_NAME = "gestion_OT_prod"


async def get_collection(db: Optional[AsyncIOMotorDatabase] = None) -> AsyncIOMotorCollection:
    database = db if db is not None else get_db()
    return database[_COLLECTION_NAME]


async def ensure_indexes(db: Optional[AsyncIOMotorDatabase] = None) -> None:
    col = await get_collection(db)
    await col.create_index("OT", unique=True, name="uq_gestion_ot_prod_ot")


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


async def insert_entry(
    entry: Dict[str, Any],
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Dict[str, Any]:
    col = await get_collection(db)
    res = await col.insert_one(entry)
    return await col.find_one({"_id": res.inserted_id})


async def list_entries(
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
    cursor = cursor.skip(int(skip)).limit(int(limit))
    return [doc async for doc in cursor]


async def update_estado_by_ot(
    ot: int,
    estado: str,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Dict[str, Any]]:
    return await update_fields_by_ot(
        ot,
        {"estado": estado},
        db=db,
    )


async def update_fields_by_ot(
    ot: int,
    fields: Dict[str, Any],
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Dict[str, Any]]:
    if not fields:
        return await find_by_ot(int(ot), db=db)

    col = await get_collection(db)
    update_fields = dict(fields)
    update_fields["audit.updatedAt"] = datetime.now(timezone.utc)
    return await col.find_one_and_update(
        {"OT": int(ot)},
        {"$set": update_fields},
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
    Útil para cerrar backlog (días anteriores y no solo ayer).
    """
    col = await get_collection(db)
    result = await col.update_many(
        {
            "contenido.fecha": {"$lt": fecha_fin_exclusive},
            "estado": {"$ne": "CERRADA"},
        },
        {
            "$set": {
                "estado": "CERRADA",
                "audit.updatedAt": datetime.now(timezone.utc),
            }
        },
    )
    return int(result.modified_count)
