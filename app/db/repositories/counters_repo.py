from __future__ import annotations

from typing import Any, Dict, Optional

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ReturnDocument

from app.db.mongo import get_db

_COLLECTION = "counters"


async def _collection(db: Optional[AsyncIOMotorDatabase] = None) -> AsyncIOMotorCollection:
    database = db if db is not None else get_db()
    return database[_COLLECTION]


async def find_by_id(
    counter_id: str,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Dict[str, Any]]:
    col = await _collection(db)
    return await col.find_one({"_id": counter_id})


async def update_seq(
    counter_id: str,
    seq: int,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Dict[str, Any]]:
    col = await _collection(db)
    return await col.find_one_and_update(
        {"_id": counter_id},
        {"$set": {"seq": int(seq)}},
        return_document=ReturnDocument.AFTER,
    )


async def increment_seq(
    counter_id: str,
    step: int = 1,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Dict[str, Any]]:
    col = await _collection(db)
    return await col.find_one_and_update(
        {"_id": counter_id},
        {"$inc": {"seq": int(step)}},
        return_document=ReturnDocument.AFTER,
    )


async def decrement_seq(
    counter_id: str,
    step: int = 1,
    *,
    floor: int = 0,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Dict[str, Any]]:
    """
    Disminuye el contador de forma atómica siempre que no quede bajo 'floor'.
    Si 'seq' es menor al paso solicitado, no modifica nada y retorna None.
    """
    col = await _collection(db)
    return await col.find_one_and_update(
        {"_id": counter_id, "seq": {"$gte": int(floor) + int(step)}},
        {"$inc": {"seq": -int(step)}},
        return_document=ReturnDocument.AFTER,
    )
