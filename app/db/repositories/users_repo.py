# app/db/repositories/users_repo.py
from __future__ import annotations

import re
from typing import Optional, List, Dict, Any, Sequence, Tuple
from datetime import datetime
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorCollection
from pymongo import ASCENDING, DESCENDING, ReturnDocument

from app.db.mongo import get_db

_COLLECTION = "users"

# ---------------- helpers -----------------
def _oid(id_str: str) -> ObjectId:
    if not ObjectId.is_valid(id_str):
        raise ValueError(f"ObjectId inválido: {id_str}")
    return ObjectId(id_str)

def _normalize_sort(sort: Optional[Sequence[Tuple[str, int]]]) -> Optional[List[Tuple[str, int]]]:
    if not sort:
        return None
    out: List[Tuple[str, int]] = []
    for field, direction in sort:
        d = ASCENDING if direction in (1, ASCENDING) else DESCENDING
        out.append((field, d))
    return out

async def coll(db: Optional[AsyncIOMotorDatabase] = None) -> AsyncIOMotorCollection:
    database = db if db is not None else get_db()
    return database[_COLLECTION]

# --------------- índices (opcional llamar al inicio) ---------------
async def ensure_indexes(db: Optional[AsyncIOMotorDatabase] = None) -> None:
    c = await coll(db)
    await c.create_index([("email_ci", ASCENDING)], unique=True, name="uq_email_ci")
    await c.create_index([("alias_ci", ASCENDING)], unique=True, name="uq_alias_ci")
    await c.create_index([("status", ASCENDING)], name="idx_status")
    await c.create_index([("role", ASCENDING)], name="idx_role")

# --------------- queries básicas ----------------
async def find_by_id(user_id: str, db: Optional[AsyncIOMotorDatabase] = None) -> Optional[Dict[str, Any]]:
    c = await coll(db)
    return await c.find_one({"_id": _oid(user_id)})

async def find_by_email(email: str, db: Optional[AsyncIOMotorDatabase] = None) -> Optional[Dict[str, Any]]:
    c = await coll(db)
    return await c.find_one({"email_ci": email.strip().lower()})

async def find_by_alias(
    alias: str,
    db:Optional[AsyncIOMotorDatabase] = None
)-> List[Dict[str, Any]]:

    c = await coll(db)
    filtro = {"alias_ci": {"$regex": f"{re.escape(alias.lower())}"}}
    cursor = c.find(filtro)

    results: List[Dict[str, Any]]=[doc async for doc in cursor]
    return results

async def find_one_by_alias(alias: str, db: Optional[AsyncIOMotorDatabase] = None) -> Optional[Dict[str, Any]]:
    c = await coll(db)
    return await c.find_one({"alias_ci": alias.strip().lower()})

async def list_users(
    *,
    filtro: Optional[Dict[str, Any]] = None,
    skip: int = 0,
    limit: int = 50,
    sort: Optional[Sequence[Tuple[str, int]]] = None,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> List[Dict[str, Any]]:
    c = await coll(db)
    q = filtro or {}
    cursor = c.find(q)
    s = _normalize_sort(sort)
    if s: cursor = cursor.sort(s)
    if skip: cursor = cursor.skip(int(skip))
    if limit: cursor = cursor.limit(int(limit))
    return [doc async for doc in cursor]

async def count_users(filtro: Optional[Dict[str, Any]] = None, db: Optional[AsyncIOMotorDatabase] = None) -> int:
    c = await coll(db)
    return await c.count_documents(filtro or {})

# --------------- escrituras ----------------
async def insert_user(doc: Dict[str, Any], db: Optional[AsyncIOMotorDatabase] = None) -> Dict[str, Any]:
    c = await coll(db)
    if "alias" in doc:
        doc["alias_ci"]=doc["alias"].lower() # Insertamos alias en minusculas en alias_ci para busqueda rapida. 
    res = await c.insert_one(doc)
    return await c.find_one({"_id": res.inserted_id})

async def update_user_by_id(
    user_id: str,
    set_fields: Dict[str, Any],
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Dict[str, Any]]:
    c = await coll(db)
    return await c.find_one_and_update(
        {"_id": _oid(user_id)},
        {"$set": set_fields},
        return_document=ReturnDocument.AFTER
    )

async def set_password_hash(
    user_id: str,
    password_hash: str,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Dict[str, Any]]:
    c = await coll(db)
    return await c.find_one_and_update(
        {"_id": _oid(user_id)},
        {"$set": {"passwordHash": password_hash, "audit.updatedAt": datetime.utcnow()}},
        return_document=ReturnDocument.AFTER
    )
