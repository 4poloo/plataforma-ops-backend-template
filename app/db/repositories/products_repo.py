# app/db/repositories/products_repo.py
# -----------------------------------------------------------------------------
# Repositorio de acceso a datos para la colección "products" (Motor + MongoDB).
# - Mantén aquí SOLO queries a la BD (lecturas/escrituras/aggregations/índices).
# - La API NO debe hablar con Mongo directo; usa un service que consuma este repo.
# -----------------------------------------------------------------------------
from __future__ import annotations

import re
from typing import Optional, Iterable, Sequence, Tuple, List, Dict, Any

from datetime import datetime
from bson import ObjectId
from motor.motor_asyncio import (
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)
from pymongo import ASCENDING, DESCENDING, UpdateOne
from pymongo.errors import BulkWriteError

from app.db.mongo import get_db

_COLLECTION_NAME = "products"
_IMPORT_BATCHES = "import_batches"  # 👈 colección temporal para batches de import

# ------------------------------ Helpers base ---------------------------------
def _parse_object_id(_id: str) -> ObjectId:
    """Valida y castea un str a ObjectId o levanta ValueError si es inválido."""
    if not ObjectId.is_valid(_id):
        raise ValueError(f"ObjectId inválido: '{_id}'")
    return ObjectId(_id)


def _wrap_update(update: Dict[str, Any]) -> Dict[str, Any]:
    """
    Asegura que el update use operadores ($set, $inc, etc.). Si el dict no
    trae operadores, se asume que el usuario quiso hacer "$set".
    """
    has_operator = any(k.startswith("$") for k in update.keys())
    return update if has_operator else {"$set": update}


def _normalize_sort(
    sort: Optional[Sequence[Tuple[str, int]]],
) -> Optional[List[Tuple[str, int]]]:
    """
    Normaliza y valida sort. Acepta [(campo, 1), (campo, -1)].
    Retorna None si no hay sort.
    """
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
    """
    Retorna la colección 'products'. Permite inyectar 'db' en tests.
    """
    database = db if db is not None else get_db()
    return database[_COLLECTION_NAME]

async def find_existing_skus(
    db: AsyncIOMotorDatabase,
    skus: list[str],
) -> set[str]:
    """
    Retorna un set con los SKU que ya existen en BD entre la lista entregada.
    """
    if not skus:
        return set()
    col = await get_collection(db)
    cursor = col.find({"sku": {"$in": skus}}, {"sku": 1})
    docs = [doc async for doc in cursor]
    return {str(d.get("sku")) for d in docs if d.get("sku")}

async def ensure_indexes(
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> None:
    """
    Crea/asegura índices útiles para queries frecuentes de forma idempotente.
    - Si ya existe un índice equivalente pero con otro nombre, no falla.
    - Si existe el de SKU pero sin 'unique', lo reconstruye como único.
    """
    col = await get_collection(db)

    # 1) Inspeccionamos índices existentes
    #    index_information() devuelve { name: { 'key': [('field', dir)], 'unique': bool, ... } }
    existing = await col.index_information()

    def has_index_with_key(key_pattern: list[tuple[str, int]]):
        for meta in existing.values():
            # meta['key'] es una lista de tuplas [('campo', 1), ...]
            if meta.get("key") == key_pattern:
                return True, meta
        return False, None

    # ---------- Índice único por SKU ----------
    desired_name = "uniq_sku"   # usa SIEMPRE el mismo nombre para evitar conflictos
    key = [("sku", ASCENDING)]

    found, meta = has_index_with_key(key)
    if found:
        # Si existe pero NO es único, lo reconstruimos
        if not meta.get("unique"):
            try:
                await col.drop_index(meta["name"])
            except Exception:
                # Si no se puede dropear por algún motivo, lo ignoramos para no bloquear el arranque.
                pass
            await col.create_index(key, name=desired_name, unique=True)
        else:
            # Ya existe y es único → nada que hacer (aunque el nombre sea distinto)
            pass
    else:
        # No existe → lo creamos
        await col.create_index(key, name=desired_name, unique=True)

    # ---------- Otros índices útiles ----------
    # categoria
    # Si ya existe con la misma key, no recreamos.
    if not has_index_with_key([("categoria", ASCENDING)])[0]:
        await col.create_index([("categoria", ASCENDING)], name="idx_categoria")

    # activo
    if not has_index_with_key([("activo", ASCENDING)])[0]:
        await col.create_index([("activo", ASCENDING)], name="idx_activo")

    # nombre (si prefieres búsquedas case-insensitive por 'nombre_ci', cambia aquí)
    if not has_index_with_key([("nombre", ASCENDING)])[0]:
        await col.create_index([("nombre", ASCENDING)], name="idx_nombre")

# ------------------------------ Lecturas -------------------------------------
async def find_by_id(
    _id: str,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
    projection: Optional[Dict[str, int]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Busca un producto por _id (string). Retorna el documento o None.
    """
    col = await get_collection(db)
    oid = _parse_object_id(_id)
    return await col.find_one({"_id": oid}, projection=projection)


async def find_by_sku(
    sku: str,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
    projection: Optional[Dict[str, int]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Busca un producto por SKU exacto.
    """
    col = await get_collection(db)
    return await col.find_one({"sku": sku}, projection=projection)


async def find_by_sku_like(
    sku_fragment: str,
    *,
    limit: int = 20,
    skip: int = 0,
    db: Optional[AsyncIOMotorDatabase] = None,
    projection: Optional[Dict[str, int]] = None,
) -> List[Dict[str, Any]]:
    """
    Busca productos cuyo SKU contenga 'sku_fragment' (case-insensitive).
    Pensado para búsquedas en vivo, incluye paginación.
    """
    col = await get_collection(db)
    filtro = {"sku": {"$regex": re.escape(sku_fragment), "$options": "i"}}
    cursor = col.find(filtro, projection=projection)
    if skip:
        cursor = cursor.skip(int(skip))
    if limit:
        cursor = cursor.limit(int(limit))
    return [doc async for doc in cursor]


async def count(
    filtro: Optional[Dict[str, Any]] = None,
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> int:
    """
    Cuenta documentos que calzan con 'filtro'.
    """
    col = await get_collection(db)
    return await col.count_documents(filtro or {})


async def find_many(
    filtro: Optional[Dict[str, Any]] = None,
    *,
    limit: int = 20,
    skip: int = 0,
    sort: Optional[Sequence[Tuple[str, int]]] = None,
    projection: Optional[Dict[str, int]] = None,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> List[Dict[str, Any]]:
    """
    Lista productos por filtro con paginado y orden.
    - filtro: dict de condiciones Mongo (ej: {"categoria": "A", "activo": True})
    - sort: lista de tuplas (campo, 1|-1) ej: [("nombre", 1), ("precio", -1)]
    """
    col = await get_collection(db)
    cursor = col.find(filtro or {}, projection=projection)

    sort_norm = _normalize_sort(sort)
    if sort_norm:
        cursor = cursor.sort(sort_norm)
    if skip:
        cursor = cursor.skip(int(skip))
    if limit:
        cursor = cursor.limit(int(limit))

    results: List[Dict[str, Any]] = [doc async for doc in cursor]
    return results


async def find_product_by_fam(
    familia: str,
    *,
    limit: int = 20,
    skip: int = 0,
    db: Optional[AsyncIOMotorDatabase] = None,
    projection: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    col = await get_collection(db)
    filtro = {"dg": {"$regex": f"{re.escape(familia.upper())}"}}
    cursor = col.find(filtro, projection=projection)
    if limit: cursor.limit(int(limit))
    if skip: cursor.skip(int(skip))
    return [doc async for doc in cursor]


async def find_product_by_subfam(
    subfamilia: str,
    *,
    limit: int = 20,
    skip: int = 0,
    db: Optional[AsyncIOMotorDatabase] = None,
    projection: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    col = await get_collection(db)
    filtro = {"dsg": {"$regex": f"{re.escape(subfamilia.upper())}"}}
    cursor = col.find(filtro, projection=projection)
    if limit: cursor.limit(int(limit))
    if skip: cursor.skip(int(skip))
    return [doc async for doc in cursor]


async def find_product_by_name(
    name: str,
    *,
    limit: int = 20,
    skip: int = 0,
    db: Optional[AsyncIOMotorDatabase] = None,
    projection: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    col = await get_collection(db)
    filtro = {"nombre": {"$regex": f"{re.escape(name.upper())}"}}
    cursor = col.find(filtro, projection=projection)
    if limit: cursor.limit(int(limit))
    if skip: cursor.skip(int(skip))
    return [doc async for doc in cursor]


async def find_product_by_type(
    tipo: str,
    *,
    limit: int = 20,
    skip: int = 0,
    db: Optional[AsyncIOMotorDatabase] = None,
    projection: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    col = await get_collection(db)
    filtro = {"tipo": {"$regex": f"{re.escape(tipo.upper())}"}}
    cursor = col.find(filtro, projection=projection)
    if limit: cursor.limit(int(limit))
    if skip: cursor.skip(int(skip))
    return [doc async for doc in cursor]


async def find_product_mixed(
    filtro: Optional[Dict[str, Any]] = None,
    *,
    limit: int = 20,
    skip: int = 0,
    db: Optional[AsyncIOMotorDatabase] = None,
    projection: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    col = await get_collection(db)
    cursor = col.find(filtro, projection=projection)
    if limit: cursor.limit(int(limit))
    if skip: cursor.skip(int(skip))
    return [doc async for doc in cursor]


# ------------------------------ Escrituras -----------------------------------
async def insert_one(
    doc: Dict[str, Any],
    *,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> str:
    """
    Inserta un producto y retorna el documento (con _id ya asignado).
    - Regla local: nombre_ci = nombre.lower() para búsquedas/orden insensible a mayúsculas.
    """
    col = await get_collection(db)
    doc['nombre_ci'] = doc['nombre'].lower()
    result = await col.insert_one(doc)
    doc['_id'] = result.inserted_id
    return doc


async def update_by_id(
    _id: str,
    update: Dict[str, Any],
    *,
    upsert: bool = False,
    db: Optional[AsyncIOMotorDatabase] = None,
) -> int:
    """
    Actualiza un producto por _id. Retorna cantidad modificada (0|1).
    Si 'update' no contiene operadores ($set, $inc, ...), se envuelve en $set.
    """
    col = await get_collection(db)
    oid = _parse_object_id(_id)
    safe_update = _wrap_update(update)
    result = await col.update_one({"_id": oid}, safe_update, upsert=upsert)
    return int(result.modified_count)


# -------------------------- IMPORT: Batches & Bulk ---------------------------
async def _batches_col(
    db: Optional[AsyncIOMotorDatabase] = None,
) -> AsyncIOMotorCollection:
    # Motor no permite truthiness: compara con None explícitamente
    database = db if db is not None else get_db()
    return database["import_batches"]


async def save_import_batch(
    db: AsyncIOMotorDatabase,
    items: List[Dict[str, Any]],
    *,
    ttl_minutes: int = 30,
) -> str:
    """
    Guarda un batch temporal de import en 'import_batches' con TTL.
    - items: [{ row, payload, errors, warnings }, ...]
    - Crea índice TTL sobre 'expiresAt' (idempotente).
    - Devuelve el batch_id (string).
    """
    from datetime import datetime, timedelta

    col = await _batches_col(db)
    batch_id = f"batch-{datetime.utcnow().timestamp()}".replace(".", "")
    doc = {
        "_id": batch_id,
        "items": items,
        "createdAt": datetime.utcnow(),
        "expiresAt": datetime.utcnow() + timedelta(minutes=ttl_minutes),
    }
    await col.insert_one(doc)
    # Índice TTL (si no existe, lo crea; si existe, no falla)
    await col.create_index("expiresAt", expireAfterSeconds=0)
    return batch_id


async def get_import_batch(
    db: AsyncIOMotorDatabase,
    batch_id: str,
) -> Optional[Dict[str, Any]]:
    """Lee un batch temporal por ID (o None si expiró o no existe)."""
    col = await _batches_col(db)
    return await col.find_one({"_id": batch_id})


async def delete_import_batch(
    db: AsyncIOMotorDatabase,
    batch_id: str,
) -> None:
    """Elimina un batch temporal (limpieza post confirm)."""
    col = await _batches_col(db)
    await col.delete_one({"_id": batch_id})


async def bulk_upsert_products_by_sku(
    db: AsyncIOMotorDatabase,
    docs: List[Dict[str, Any]],
) -> Tuple[int, int]:
    """
    Realiza un bulk upsert por clave 'sku'.
    - docs deben venir listos para persistir (con 'sku' y demás campos mapeados).
    - Retorna (created, updated).
    """
    if not docs:
        return (0, 0)

    col = await get_collection(db)
    ops: List[UpdateOne] = []

    for d in docs:
        sku = (d.get("sku") or "").strip().upper()
        if not sku:
            # Si viene sin SKU lo saltamos (y que el router lo cuente como skipped si quiere)
            continue
        # upsert por SKU
        ops.append(UpdateOne({"sku": sku}, {"$set": d}, upsert=True))

    if not ops:
        return (0, 0)

    try:
        result = await col.bulk_write(ops, ordered=False)
        created = result.upserted_count or 0
        updated = result.modified_count or 0
        return (created, updated)
    except BulkWriteError as bwe:
        # Propagamos el error para que el router decida cómo responder.
        raise bwe

async def confirm_import_batch(
    db: AsyncIOMotorDatabase,
    batch_id: str,
) -> dict:
    """
    Confirma un batch de importación:
      - Inserta los ítems válidos (sin errores) que no existan por SKU -> created
      - Actualiza los que existan -> updated
      - Si no hay cambios efectivos -> skipped
      - Fuerza activo=True y setea/actualiza 'nombre_ci' y timestamps.
    Retorna: { ok, created, updated, skipped }
    """
    col = await get_collection(db)

    # Cargamos el batch
    batches = db.get_collection("import_batches")
    batch = await batches.find_one({"_id": batch_id})
    if not batch:
        return {"ok": False, "created": 0, "updated": 0, "skipped": 0, "message": "Batch no encontrado"}

    created = updated = skipped = 0

    items = batch.get("items", [])
    for it in items:
        payload: dict = it.get("payload") or {}
        errors: list = it.get("errors") or []
        if errors:
            # Filas inválidas no se procesan
            continue

        sku = (payload.get("sku") or "").strip()
        if not sku:
            # Sin SKU no se procesa
            continue

        # Normalizaciones y campos derivados
        doc = {**payload}
        doc["activo"] = True
        if "nombre" in doc and isinstance(doc["nombre"], str):
            doc["nombre_ci"] = doc["nombre"].lower()

        now = datetime.utcnow()
        # createdAt solo para insert; updatedAt para ambos casos
        doc.setdefault("createdAt", now)
        doc["updatedAt"] = now

        # ¿Existe por SKU?
        current = await col.find_one({"sku": sku})

        if not current:
            # INSERT
            await col.insert_one(doc)
            created += 1
        else:
            # UPDATE (solo si hay cambios efectivos)
            # Construimos set de cambios comparando 'current' vs 'doc'
            set_changes = {}
            for k, v in doc.items():
                # evitamos tocar _id
                if k == "_id":
                    continue
                if current.get(k) != v:
                    set_changes[k] = v

            if set_changes:
                await col.update_one({"_id": current["_id"]}, {"$set": set_changes})
                updated += 1
            else:
                skipped += 1

    # Limpieza opcional del batch
    await batches.delete_one({"_id": batch_id})

    return {"ok": True, "created": created, "updated": updated, "skipped": skipped}
