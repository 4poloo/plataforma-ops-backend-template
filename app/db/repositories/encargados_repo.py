from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ASCENDING

from app.db.mongo import get_db

_COLLECTION_NAME = "encargados"


async def get_collection(db: Optional[AsyncIOMotorDatabase] = None) -> AsyncIOMotorCollection:
    database = db if db is not None else get_db()
    return database[_COLLECTION_NAME]


def _parse_object_id(_id: str | ObjectId) -> ObjectId:
    if isinstance(_id, ObjectId):
        return _id
    if not ObjectId.is_valid(_id):
        raise ValueError("ObjectId inválido")
    return ObjectId(_id)


async def ensure_indexes(*, db: Optional[AsyncIOMotorDatabase] = None) -> None:
    col = await get_collection(db)
    await col.create_index([("nombre", ASCENDING), ("linea", ASCENDING)], name="idx_nombre_linea")


async def insert_encargado(doc: Dict[str, Any], *, db: Optional[AsyncIOMotorDatabase] = None) -> Dict[str, Any]:
    col = await get_collection(db)
    res = await col.insert_one(doc)
    return await col.find_one({"_id": res.inserted_id})


async def find_all(
    *,
    filtro: Optional[Dict[str, Any]] = None,
    limit: int = 100,
    skip: int = 0,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> List[Dict[str, Any]]:
    col = await get_collection(db)
    cursor = col.find(filtro or {}).sort("nombre", ASCENDING).skip(int(skip)).limit(int(limit))
    return [doc async for doc in cursor]


async def find_by_id(
    _id: str | ObjectId,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Dict[str, Any]]:
    col = await get_collection(db)
    oid = _parse_object_id(_id)
    return await col.find_one({"_id": oid})


async def update_encargado(
    _id: str | ObjectId,
    update: Dict[str, Any],
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Dict[str, Any]]:
    col = await get_collection(db)
    oid = _parse_object_id(_id)
    await col.update_one({"_id": oid}, {"$set": update})
    return await col.find_one({"_id": oid})


async def find_by_nombre_linea(
    nombre: str,
    linea: str,
    *,
    exclude_id: Optional[str | ObjectId] = None,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Dict[str, Any]]:
    col = await get_collection(db)
    filtro: Dict[str, Any] = {"nombre": nombre, "linea": linea}
    if exclude_id:
        filtro["_id"] = {"$ne": _parse_object_id(exclude_id)}
    return await col.find_one(filtro)

