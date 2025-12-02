# app/db/repositories/recipes_repo.py
from __future__ import annotations

import re
from typing import Optional, Iterable, Sequence, Tuple, List, Dict, Any
from datetime import datetime
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING

from app.db.mongo import get_db

_COLLECTION_NAME = "recipes"

# ------------------------------ Helpers base ---------------------------------
def _parse_object_id(_id: str) -> ObjectId:
    if not ObjectId.is_valid(_id):
        raise ValueError(f"ObjectId inválido: '{_id}'")
    return ObjectId(_id)

def _wrap_update(update: Dict[str, Any]) -> Dict[str, Any]:
    has_operator = any(k.startswith("$") for k in update.keys())
    return update if has_operator else {"$set": update}

def _normalize_sort(
    sort: Optional[Sequence[Tuple[str, int]]],
) -> Optional[List[Tuple[str, int]]]:
    if not sort:
        return None
    normalized: List[Tuple[str, int]] = []
    for field, direction in sort:
        if direction not in (1, -1, ASCENDING, DESCENDING):
            raise ValueError(f"Dirección de sort inválida para '{field}': {direction}")
        dir_norm = ASCENDING if direction in (1, ASCENDING) else DESCENDING
        normalized.append((field, dir_norm))
    return normalized

async def get_collection(
    db: Optional[AsyncIOMotorDatabase] = None,
) -> AsyncIOMotorCollection:
    database = db if db is not None else get_db()
    return database[_COLLECTION_NAME]

# ---------------------- Colecciones relacionadas -----------------------------
async def products_coll(db: Optional[AsyncIOMotorDatabase] = None):
    database = db if db is not None else get_db()
    return database["products"]

async def processes_coll(db: Optional[AsyncIOMotorDatabase] = None):
    database = db if db is not None else get_db()
    return database["processes"]

# ---------------------- Lookups auxiliares (products/processes) --------------
async def get_product_by_sku(sku: str, db: Optional[AsyncIOMotorDatabase] = None) -> Optional[Dict[str, Any]]:
    col = await products_coll(db)
    return await col.find_one({"sku": sku})

async def get_pt_by_sku(sku: str, db: Optional[AsyncIOMotorDatabase] = None) -> Optional[Dict[str, Any]]:
    col = await products_coll(db)
    return await col.find_one({"sku": sku, "tipo": "PT"})

async def get_process_by_code(code: str, db: Optional[AsyncIOMotorDatabase] = None) -> Optional[Dict[str, Any]]:
    col = await processes_coll(db)
    return await col.find_one({"codigo": code})

# NUEVO: obtener proceso por _id (para valorización)
async def get_process_by_id(_id: Any, db: Optional[AsyncIOMotorDatabase] = None) -> Optional[Dict[str, Any]]:
    col = await processes_coll(db)
    oid = _id if isinstance(_id, ObjectId) else (ObjectId(_id) if ObjectId.is_valid(str(_id)) else None)
    if not oid:
        return None
    return await col.find_one({"_id": oid})

# NUEVO: traer productos por lista de ObjectId (para valorización)
async def find_products_by_ids(
    ids: List[ObjectId],
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
    projection: Optional[Dict[str, int]] = None,
) -> List[Dict[str, Any]]:
    if not ids:
        return []
    col = await products_coll(db)
    cursor = col.find({"_id": {"$in": ids}}, projection=projection)
    return [doc async for doc in cursor]
#----------------------- Helpers de versiones-----------------------------------------------
async def update_version_estado(
    recipe_id: ObjectId,
    version_num: int,
    nuevo_estado: str,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Dict[str, Any]:
    col = await get_collection(db)
    await col.update_one(
        {"_id": recipe_id, "versiones.version": int(version_num)},
        {"$set": {"versiones.$.estado": nuevo_estado, "audit.updatedAt": datetime.utcnow()}},
    )
    return await col.find_one({"_id": recipe_id})

async def clear_vigente_version(
    recipe_id: ObjectId,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Dict[str, Any]:
    col = await get_collection(db)
    await col.update_one(
        {"_id": recipe_id},
        {"$unset": {"vigenteVersion": ""}, "$set": {"audit.updatedAt": datetime.utcnow()}},
    )
    return await col.find_one({"_id": recipe_id})
# ----------------------[CRUD] existentes + utilidades -------------------------
async def find_by_id(
    _id: str,
    db: Optional[AsyncIOMotorDatabase] = None,
    projection: Optional[Dict[str, int]] = None,
) -> Optional[Dict[str, Any]]:
    coll = await get_collection(db)
    oid = _parse_object_id(_id)
    return await coll.find_one({"_id": oid}, projection=projection)

async def find_all(
    filtro: Optional[Dict[str, Any]] = None,
    *,
    limit=20,
    skip=0,
    sort: Optional[Sequence[Tuple[str, int]]] = None,
    projection: Optional[Dict[str, int]] = None,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> List[Dict[str, Any]]:
    coll = await get_collection(db)
    cursor = coll.find(filtro or {}, projection=projection)
    sort_norm = _normalize_sort(sort)
    if sort_norm:
        cursor = cursor.sort(sort_norm)
    if skip:
        cursor = cursor.skip(int(skip))
    if limit:
        cursor = cursor.limit(int(limit))
    return [doc async for doc in cursor]

async def find_by_name(
    name: str,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
    projection: Optional[Dict[str, int]] = None,
) -> Optional[Dict[str, Any]]:
    coll = await get_collection(db)
    return await coll.find_one({"nombre": name}, projection=projection)

async def find_like(
    name: str,
    *,
    limit: int = 20,
    skip: int = 0,
    db: Optional[AsyncIOMotorDatabase] = None,
    projection: Optional[Dict[str, int]] = None,
) -> List[Dict[str, Any]]:
    coll = await get_collection(db)
    filtro = {"nombre_ci": {"$regex": f"{re.escape(name.lower())}"}}
    cursor = coll.find(filtro, projection=projection)
    if limit:
        cursor = cursor.limit(int(limit))
    if skip:
        cursor = cursor.skip(int(skip))
    return [doc async for doc in cursor]

async def find_product_mixed(
    filtro: Optional[Dict[str, Any]] = None,
    *,
    limit: int = 20,
    skip: int = 0,
    db: Optional[AsyncIOMotorDatabase] = None,
    projection: Optional[Dict[str, int]] = None,
) -> List[Dict[str, Any]]:
    col = await get_collection(db)
    cursor = col.find(filtro or {}, projection=projection)
    if limit:
        cursor = cursor.limit(int(limit))
    if skip:
        cursor = cursor.skip(int(skip))
    return [doc async for doc in cursor]

async def update_by_id(
    _id: str,
    update: Dict[str, Any],
    *,
    upsert: bool = False,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> int:
    col = await get_collection(db)
    oid = _parse_object_id(_id)
    safe_update = _wrap_update(update)
    result = await col.update_one({"_id": oid}, safe_update, upsert=upsert)
    return int(result.modified_count)

# ---------------------- Nuevas funciones usadas por el service ---------------
async def find_by_pt_id(
    pt_id: ObjectId,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Dict[str, Any]]:
    col = await get_collection(db)
    return await col.find_one({"productPTId": pt_id})

async def insert_recipe(
    recipe_doc: Dict[str, Any],
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Dict[str, Any]:
    col = await get_collection(db)
    res = await col.insert_one(recipe_doc)
    return await col.find_one({"_id": res.inserted_id})

async def push_recipe_version(
    recipe_id: ObjectId,
    version_doc: Dict[str, Any],
    *,
    marcar_vigente: bool,
    updated_at: datetime,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Dict[str, Any]:
    col = await get_collection(db)
    update: Dict[str, Any] = {
        "$push": {"versiones": version_doc},
        "$set": {"audit.updatedAt": updated_at},
    }
    if marcar_vigente:
        update["$set"]["vigenteVersion"] = version_doc["version"]
    await col.update_one({"_id": recipe_id}, update)
    return await col.find_one({"_id": recipe_id})

async def set_recipe_meta(
    recipe_id: ObjectId,
    *,
    vigente_version: Optional[int],
    updated_at: datetime,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Dict[str, Any]:
    col = await get_collection(db)
    set_fields: Dict[str, Any] = {"audit.updatedAt": updated_at}
    if vigente_version is not None:
        set_fields["vigenteVersion"] = int(vigente_version)
    await col.update_one({"_id": recipe_id}, {"$set": set_fields})
    return await col.find_one({"_id": recipe_id})

async def update_version_fields(
    recipe_id: ObjectId,
    version_num: int,
    set_fields: Dict[str, Any],
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Dict[str, Any]:
    col = await get_collection(db)
    await col.update_one(
        {"_id": recipe_id, "versiones.version": int(version_num)},
        {"$set": set_fields},
    )
    return await col.find_one({"_id": recipe_id})

async def replace_version_components(
    recipe_id: ObjectId,
    version_num: int,
    componentes: List[Dict[str, Any]],
    *,
    updated_at: datetime,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Dict[str, Any]:
    col = await get_collection(db)
    base = f"versiones.$.componentes"
    await col.update_one(
        {"_id": recipe_id, "versiones.version": int(version_num)},
        {"$set": {base: componentes, "audit.updatedAt": updated_at}},
    )
    return await col.find_one({"_id": recipe_id})

# --- STAGING ---
async def staging_coll(db: Optional[AsyncIOMotorDatabase] = None):
    return (db or get_db())["staging_recipes"]

_STAGING_ALLOWED = {
    "sku_PT","version","estado","marcar_vigente","base_qty","unidad_PT",
    "sku_MP","cantidad_por_base","unidad_MP","merma_pct",
    "process_codigo","process_especial_nombre","process_especial_costo",
    "fecha_publicacion","publicado_por","notas"
}

def _clean_row(raw: Dict[str, Any]) -> Dict[str, Any]:
    # Normaliza claves desconocidas fuera, trim de strings
    out: Dict[str, Any] = {}
    for k, v in raw.items():
        k2 = k.strip()
        if k2 in _STAGING_ALLOWED:
            if isinstance(v, str):
                v = v.strip()
            out[k2] = v
    return out

async def stage_insert_rows(rows: List[Dict[str, Any]], *, batch_id: str, db=None) -> Tuple[int, List[str]]:
    col = await staging_coll(db)
    docs = []
    warnings: List[str] = []
    for r in rows:
        d = _clean_row(r)
        d["batch_id"] = batch_id
        if not d.get("sku_PT") or not d.get("version"):
            warnings.append(f"Fila omitida por faltar sku_PT/version: {r}")
            continue
        docs.append(d)
    if not docs:
        return 0, warnings
    res = await col.insert_many(docs)
    return len(res.inserted_ids), warnings

async def stage_status(*, batch_id: str, db=None) -> Tuple[int, List[Dict[str, Any]]]:
    col = await staging_coll(db)
    total = await col.count_documents({"batch_id": batch_id})
    cursor = col.find({"batch_id": batch_id}).limit(5)
    sample = [doc async for doc in cursor]
    for s in sample:
        s.pop("_id", None)
    return total, sample

async def stage_clear(*, batch_id: str, db=None) -> int:
    col = await staging_coll(db)
    res = await col.delete_many({"batch_id": batch_id})
    return int(res.deleted_count)

# ---------------------- Nuevas funciones: actualizar nombre de receta --------

async def set_recipe_name_by_pt_id(
    pt_id: ObjectId,
    nombre: str,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> int:
    """
    Actualiza 'nombre' en la receta asociada a productPTId = pt_id.
    Retorna modified_count.
    """
    col = await get_collection(db)
    res = await col.update_one(
        {"productPTId": pt_id},
        {"$set": {"nombre": nombre, "nombre_ci": nombre.lower(), "audit.updatedAt": datetime.utcnow()}},
    )
    return int(res.modified_count)

async def set_recipe_name_by_sku(
    sku_pt: str,
    nombre: str,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> int:
    """
    Resuelve el PT por SKU (en 'products' con tipo:'PT') y actualiza la receta por productPTId.
    Retorna modified_count (0 si no se encontró PT o receta).
    """
    prod_col = await products_coll(db)
    pt = await prod_col.find_one({"sku": sku_pt, "tipo": "PT"}, {"_id": 1})
    if not pt or not pt.get("_id"):
        return 0
    return await set_recipe_name_by_pt_id(pt["_id"], nombre, db=db)