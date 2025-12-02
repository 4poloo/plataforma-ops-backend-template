from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING

from app.db.mongo import get_db

_COLLECTION = "logs"


async def coll(db: Optional[AsyncIOMotorDatabase] = None) -> AsyncIOMotorCollection:
    database = db if db is not None else get_db()
    return database[_COLLECTION]


def _normalize_sort(
    sort: Optional[Sequence[Tuple[str, int]]]
) -> Optional[List[Tuple[str, int]]]:
    if not sort:
        return None
    normalized: list[Tuple[str, int]] = []
    for field, direction in sort:
        normalized.append(
            (field, ASCENDING if direction in (1, ASCENDING) else DESCENDING)
        )
    return normalized


async def ensure_indexes(db: Optional[AsyncIOMotorDatabase] = None) -> None:
    c = await coll(db)
    await c.create_index([("loggedAt", DESCENDING)], name="idx_loggedAt")
    await c.create_index([("severity", ASCENDING)], name="idx_severity")
    await c.create_index([("userAlias_ci", ASCENDING)], name="idx_userAlias_ci")


async def insert_log(
    doc: Dict[str, Any], db: Optional[AsyncIOMotorDatabase] = None
) -> Dict[str, Any]:
    c = await coll(db)
    res = await c.insert_one(doc)
    return await c.find_one({"_id": res.inserted_id})


async def list_logs(
    filtro: Optional[Dict[str, Any]] = None,
    *,
    skip: int = 0,
    limit: int = 50,
    sort: Optional[Sequence[Tuple[str, int]]] = None,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> List[Dict[str, Any]]:
    c = await coll(db)
    query = filtro or {}
    cursor = c.find(query)
    normalized_sort = _normalize_sort(sort)
    if normalized_sort:
        cursor = cursor.sort(normalized_sort)
    if skip:
        cursor = cursor.skip(int(skip))
    if limit:
        cursor = cursor.limit(int(limit))
    return [doc async for doc in cursor]


async def count_logs(
    filtro: Optional[Dict[str, Any]] = None,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> int:
    c = await coll(db)
    return await c.count_documents(filtro or {})
