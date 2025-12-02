from __future__ import annotations

from typing import Iterable, Optional, Set, Any

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase

from app.db.mongo import get_db

_COLLECTION_NAME = "exclude_skus"


def _normalize_sku_for_query(value: Any) -> Set[Any]:
    """
    Returns possible representations for a SKU so the query matches numeric or string values.
    """
    if value is None:
        return set()

    text = str(value).strip()
    if not text:
        return set()

    options: Set[Any] = {text}
    try:
        options.add(int(text))
    except (TypeError, ValueError):
        pass
    return options


async def get_collection(db: Optional[AsyncIOMotorDatabase] = None) -> AsyncIOMotorCollection:
    database = db if db is not None else get_db()
    return database[_COLLECTION_NAME]


async def find_matching_skus(
    skus: Iterable[str],
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> set[str]:
    """
    Returns the SKUs present in the 'exclude_skus' collection that intersect with the provided list.
    """
    candidates: Set[Any] = set()
    for sku in skus:
        candidates.update(_normalize_sku_for_query(sku))

    if not candidates:
        return set()

    col = await get_collection(db)
    cursor = col.find({"sku": {"$in": list(candidates)}}, {"sku": 1})
    excluded: Set[str] = set()
    async for doc in cursor:
        if doc.get("sku") is None:
            continue
        excluded.add(str(doc.get("sku")))
    return excluded
