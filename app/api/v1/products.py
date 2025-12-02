# app/api/v1/products.py
# -----------------------------------------------------------------------------
# Router de Products (endpoints HTTP).
# - NO habla directo con Mongo: usa el repositorio (products_repo).
# - Serializa ObjectId -> str con un helper (_to_out).
# - Incluye paginación y orden opcional.
# -----------------------------------------------------------------------------

from __future__ import annotations

import re
import io, csv
from uuid import uuid4
from typing import List, Optional, Literal, Any, Dict 
from datetime import datetime, timedelta
from bson import ObjectId

from fastapi import APIRouter, HTTPException, Query, Path, UploadFile, File, Body, Depends
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, field_validator
from pymongo.errors import DuplicateKeyError, BulkWriteError
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.mongo import get_db                 
from app.db.repositories import products_repo    
from app.domain.familias_map import resolve_codes

router = APIRouter(prefix="/products", tags=["products"])
# ----------------------- Modelos de salida (Pydantic) ------------------------
class ProductOut(BaseModel):
    """
    Modelo de respuesta mínimo.
    - Declaramos 'id' como string (mapeado desde Mongo '_id').
    - Permitimos campos extra porque cada producto puede tener más atributos
      (sku, nombre, categoría, stock, etc.) y no queremos romper la respuesta.
    """
    id: str
    model_config = ConfigDict(extra="allow")  # acepta campos adicionales

class ProductPatch(BaseModel):
    nombre: Optional[str] = None
    sku: Optional[str] = None
    c_barra: Optional[int] = None
    unidad: Optional[str] = None
    dg: Optional[str] = None
    dsg: Optional[str] = None
    codigo_g: Optional[int] = None
    codigo_sg: Optional[int] = None
    pneto: Optional[int] = None
    piva: Optional[int] = None
    tipo: Optional[str] = None
    activo: Optional[bool] = None
    valor_repo: Optional[int] = None

    @field_validator("nombre")
    @classmethod
    def nombre_no_vacio(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            raise ValueError("nombre no puede ser vacío")
        return v

class ProductActive(BaseModel):
    activo: bool

class ProductCreate(BaseModel):
    nombre: str
    sku: str
    c_barra: int
    unidad: str
    dg: Optional[str]
    dsg: Optional[str]
    codigo_g: Optional[int]
    codigo_sg: Optional[int]
    pneto: int
    piva: int
    tipo: str
    activo: bool
    valor_repo: Optional[str]

    @field_validator("nombre")
    @classmethod
    def nombre_no_vacio(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            raise ValueError("nombre no puede ser vacío")
        return v
# --------------------------- Helpers de serialización ------------------------
def _to_out(doc: dict) -> ProductOut:
    """
    Transforma el documento Mongo en un objeto JSON-friendly.
    - Convierte '_id' (ObjectId) a 'id' (str).
    - Deja el resto de campos tal cual.
    """
    if not doc:
        raise ValueError("Documento vacío")
    data = {**doc}
    if "_id" in data:
        data["id"] = str(data.pop("_id"))
    return ProductOut(**data)
# --------------------------- Helper de Import ----------------------------
REQUIRED_HEADERS = {
    "SKU", "CODIGO_BARRA", "NOMBRE", "UNIDAD_MEDIDA",
    "NOMBRE_GRUPO", "NOMBRE_SUBGRUPO",
    "PRECIO_NETO", "CLASIFICACION",
}
PREVIEW_LIMIT = 50
MAX_ROWS = 5000

def _to_int(v: Any, default: int = 0) -> int:
    try:
        if v is None or str(v).strip() == "":
            return default
        return int(float(str(v).replace(",", ".").strip()))
    except:
        return default

# -------------------------------- Endpoints ----------------------------------

# -------------------------------- Busqueda ----------------------------------
@router.get("/by-id/{product_id}")
async def get_product_by_id(product_id: str, db=Depends(get_db)):
    # Validación de ObjectId
    if not ObjectId.is_valid(product_id):
        raise HTTPException(status_code=422, detail="ObjectId inválido")

    doc = await db["products"].find_one({"_id": ObjectId(product_id)}, {"sku": 1, "nombre": 1, "tipo": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="Producto no encontrado")

    # Serializamos _id -> id por consistencia
    return {
        "id": str(doc["_id"]),
        "sku": doc.get("sku"),
        "nombre": doc.get("nombre"),
        "tipo": doc.get("tipo"),
    }

@router.get("", response_model=List[ProductOut])
async def list_products(
    q: Optional[str] = Query(
        None,
        description="Filtro simple por nombre (regex, case-insensitive)."
    ),
    limit: int = Query(20, ge=1, le=1000, description="Máximo de items a devolver."),
    skip: int = Query(0, ge=0, description="Desplazamiento para paginado."),
    sort_field: Optional[str] = Query(
        None, description="Campo por el que ordenar (ej: 'nombre', 'sku')."
    ),
    sort_dir: int = Query(
        1, description="Dirección de orden: 1 asc, -1 desc.",
    ),
):
    """
    GET /products
    - Construye un filtro simple (por nombre si viene 'q').
    - Llama al repositorio para traer documentos paginados y ordenados.
    - Serializa cada doc para exponer 'id' en lugar de '_id'.
    """
    filtro = {}
    if q:
        # Búsqueda por nombre con regex (case-insensitive)
        filtro = {"nombre": {"$regex": q, "$options": "i"}}

    sort = [(sort_field, sort_dir)] if (sort_field is not None and sort_dir is not None) else None

    docs = await products_repo.find_many(
        filtro=filtro,
        limit=limit,
        skip=skip,
        sort=sort,
    )
    return [_to_out(d) for d in docs]


@router.get("/{product_id}", response_model=ProductOut)
async def get_product_by_id(product_id: str):
    """
    GET /products/{product_id}
    - Busca por _id (string). Si no existe, responde 404.
    """
    doc = await products_repo.find_by_id(product_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Producto no encontrado")
    return _to_out(doc)


@router.get("/by-sku/{sku}", response_model=List[ProductOut])
async def search_products_by_sku(
    sku: str,
    limit: int = Query(20, ge=1, le=1000, description="Máximo de items a devolver."),
    skip: int = Query(0, ge=0, description="Desplazamiento para paginado."),
):
    """
    GET /products/by-sku/{sku}
    - Busca productos cuyo SKU contenga el fragmento indicado (case-insensitive).
    - Pensado para búsquedas en vivo, soporta paginación con limit/skip.
    """
    docs = await products_repo.find_by_sku_like(
        sku,
        limit=limit,
        skip=skip,
    )
    if not docs:
        raise HTTPException(status_code=404, detail="Producto(s) no encontrado(s).")
    return [_to_out(d) for d in docs]

@router.get("/by-familia/{familia}", response_model=List[ProductOut])
async def get_product_by_fam(
    familia: str,
    limit: int = Query(20, ge=1, le=1000, description="Máximo de items a devolver."),
    skip: int = Query(0, ge=0, description="Desplazamiento para paginado."),
    ):
    docs = await products_repo.find_product_by_fam(
        familia,
        limit=limit,
        skip=skip
    )
    if not docs:
        raise HTTPException(status_code=404, detail="Producto(s) no encontrado(s).")
    return[_to_out(d) for d in docs]

@router.get("/by-subfamilia/{subfamilia}", response_model=List[ProductOut])
async def get_product_by_subfam(
    subfamilia: str,
    limit: int = Query(20, ge=1, le=1000, description="Máximo de items a devolver."),
    skip: int = Query(0, ge=0, description="Desplazamiento para paginado."),
    ):
    docs = await products_repo.find_product_by_subfam(
        subfamilia,
        limit=limit,
        skip=skip
    )
    if not docs:
        raise HTTPException(status_code=404, detail="Producto(s) no encontrado(s).")
    return[_to_out(d) for d in docs]

@router.get("/by-name/{name}", response_model=List[ProductOut])
async def get_product_by_name(
    name: str,
    limit: int = Query(20, ge=1, le=1000, description="Máximo de items a devolver."),
    skip: int = Query(0, ge=0, description="Desplazamiento para paginado."),
    ):
    docs = await products_repo.find_product_by_name(
        name,
        limit=limit,
        skip=skip
    )
    if not docs:
        raise HTTPException(status_code=404, detail="Producto(s) no encontrado(s).")
    return[_to_out(d) for d in docs]

@router.get("/by-type/{tipo}", response_model=List[ProductOut])
async def get_product_by_name(
    tipo: str,
    limit: int = Query(20, ge=1, le=1000, description="Máximo de items a devolver."),
    skip: int = Query(0, ge=0, description="Desplazamiento para paginado."),
    ):
    docs = await products_repo.find_product_by_type(
        tipo,
        limit=limit,
        skip=skip
    )
    if not docs:
        raise HTTPException(status_code=404, detail="Producto(s) no encontrado(s).")
    return[_to_out(d) for d in docs]

@router.get("/by-mixed/", response_model=List[ProductOut])
async def find_product_mixed(
    name: str | None = Query(None),
    dg: str | None = Query(None),
    dsg: str | None = Query(None),
    tipo: str | None = Query(None),
    activo: bool | None = Query(None),
    limit: int = Query(20, ge=1, le=1000, description="Máximo de items a devolver."),
    skip: int = Query(0, ge=0, description="Desplazamiento para paginado."),
    ):

    filtro: dict = {}
    if name:
        filtro["nombre"]= {"$regex": f"{re.escape(name.upper())}"}
    if dg:
        filtro["dg"]={"$regex": f"{re.escape(dg.upper())}"}
    if dsg:
        filtro["dsg"]={"$regex": f"{re.escape(dsg.upper())}"}
    if tipo:
        filtro["tipo"]=tipo.upper()
    if activo is not None:
        filtro["activo"]=activo

    docs = await products_repo.find_product_mixed(
        filtro,
        limit=limit,
        skip=skip
    )
    if not docs:
        raise HTTPException(status_code=404, detail="Producto(s) no encontrado(s).")
    return[_to_out(d) for d in docs]

# -------------------------------- Actualizar ----------------------------------
@router.patch("/upd/{id}", response_model=ProductOut)
async def update_by_id(
    id: str = Path(..., description="ObjectId del producto"),
    body: ProductPatch = ...,
):
    # 1) Tomar SOLO los campos enviados
    changes = body.model_dump(exclude_unset=True,exclude_none=True)
    if not changes:
        raise HTTPException(status_code=400, detail="No hay campos para actualizar")
    
    # si viene algo de familia/subfamilia, normaliza
    if any(k in changes for k in ("dg", "dsg", "codigo_g", "codigo_sg")):
        c_g, c_sg, dg_res, dsg_res = resolve_codes(
            changes.get("dg"), changes.get("dsg"),
            changes.get("codigo_g"), changes.get("codigo_sg")
        )
        changes["dg"] = dg_res.upper() or ""
        changes["codigo_g"] = int(c_g or 0)
        changes["dsg"] = dsg_res.upper() or ""
        changes["codigo_sg"] = int(c_sg or 0)

    to_unset = {k: "" for k, v in changes.items() if v is None}
    to_set   = {k: v  for k, v in changes.items() if v is not None}

     # 2) Derivados/control (agrega a to_set, no a 'data')
    if "nombre" in to_set:
        to_set["nombre_ci"] = to_set["nombre"].lower()
    if "nombre" in to_unset:
        # si borras nombre, también borra nombre_ci
        to_unset["nombre_ci"] = ""

    # (opcional) Campos que NO se pueden actualizar
    protected = {"_id", "id"}
    if any(k in protected for k in changes):
        raise HTTPException(status_code=400, detail="Campo(s) no permitidos en update")

    # 4) Construye el documento de update (solo operadores)
    update_doc: dict = {}
    if to_set:   update_doc["$set"] = to_set
    if to_unset: update_doc["$unset"] = to_unset

    if not update_doc:  # por seguridad
        raise HTTPException(status_code=400, detail="Nada que actualizar")
    
    # 4) Ejecuta update
    modified = await products_repo.update_by_id(id, update_doc)

    # 5) Lee y retorna
    doc = await products_repo.find_by_id(id)
    if not doc:
        raise HTTPException(status_code=404, detail="Producto no encontrado")
    return _to_out(doc)
# -------------------------------- Crear ----------------------------------
@router.post("/Create/", response_model=ProductOut)
async def insert_one(
    data: ProductCreate = ...,
):
    # 1) Tomar SOLO los campos enviados
    data = data.model_dump(exclude_unset=True,exclude_none=True)
    if not data:
        raise HTTPException(status_code=400, detail="No hay campos para crear.")

    c_g, c_sg, dg_res, dsg_res = resolve_codes(
        data.get("dg"), data.get("dsg"),
        data.get("codigo_g"), data.get("codigo_sg")
    )
    data["dg"] = dg_res or ""
    data["codigo_g"] = int(c_g or 0)
    data["dsg"] = dsg_res or ""
    data["codigo_sg"] = int(c_sg or 0)

    data.setdefault("activo", True)  # forzamos activo por defecto

    # 3) Ejecuta create
    try:
        create = await products_repo.insert_one(data)
    except DuplicateKeyError:
        raise HTTPException(
            status_code=409,
            detail="Ya existe un producto con ese SKU."
        )
    return _to_out(create)

# -------------------------------- Importar ----------------------------------
@router.get("/import/template")
def get_import_template_csv():
    """
    Devuelve una plantilla CSV **separada por ';'** con una fila de ejemplo.
    """
    headers = [
        "SKU","CODIGO_BARRA","NOMBRE","UNIDAD_MEDIDA",
        "NOMBRE_GRUPO","CODIGO_GRUPO","NOMBRE_SUBGRUPO","CODIGO_SUBGRUPO",
        "PRECIO_NETO","VALOR_REPOSICION","CLASIFICACION"
    ]
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, delimiter=";") #Se fuerza
    writer.writerow(headers)
    writer.writerow([
        "PT-0001","7801234567890","Detergente concentrado 5L","UN",
        "Limpieza","1","Detergentes","10",
        "4500","3800","PT"
    ])
    writer.writerow(["Este es un ejemplo, debes borrarlo antes de enviarlo, recuerda se debe enviar en formato .csv separado por punto y coma."])
    writer.writerow(["En tipo solo existe PT y MP, deja PT (Producto Terminado) para productos con receta y MP (Materia Prima) productos sin receta."])
    buf.seek(0)
    return StreamingResponse(
        io.BytesIO(buf.getvalue().encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="plantilla_import_productos.csv"'}
    )

@router.post("/import/validate")
async def import_validate(
    file: UploadFile = File(...),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Valida un CSV separado por ';', arma preview y guarda un batch temporal (TTL).
    - No inserta nada todavía en 'products'; solo prepara el batch.
    """
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Solo se acepta CSV con separador ';'.")

    raw = await file.read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("latin-1")

    first_line = text.splitlines()[0] if text else ""
    if ";" not in first_line:
        raise HTTPException(status_code=400, detail="El archivo debe usar ';' como separador.")

    reader = csv.reader(io.StringIO(text), delimiter=';')
    rows = list(reader)
    if not rows:
        raise HTTPException(status_code=400, detail="El archivo está vacío.")

    headers = [h.strip() for h in rows[0]]
    missing = [h for h in REQUIRED_HEADERS if h not in headers]
    if missing:
        raise HTTPException(
            status_code=422,
            detail=f"Faltan columnas requeridas: {', '.join(missing)}"
        )

    idx = {h: i for i, h in enumerate(headers)}
    data_rows = rows[1:1+MAX_ROWS]

    # ─────────────────────────────────────────────────────────────
    # NUEVO: pre-pase para consultar SKUs existentes en la BD
    # ─────────────────────────────────────────────────────────────
    skus_in_file: list[str] = []
    for row in data_rows:
        i = idx.get("SKU")
        sku_val = (row[i] if (i is not None and i < len(row)) else "") or ""
        skus_in_file.append(sku_val.strip().upper())

    existing_skus = await products_repo.find_existing_skus(
        db,
        list({s for s in skus_in_file if s})
    )
    # ─────────────────────────────────────────────────────────────

    preview: List[Dict[str, Any]] = []
    items_for_batch: List[Dict[str, Any]] = []
    errorsByRow: Dict[int, List[str]] = {}
    warningsByRow: Dict[int, List[str]] = {}
    seen_skus = set()

    for r_index, row in enumerate(data_rows, start=1):
        def val(col: str) -> Optional[str]:
            i = idx.get(col)
            if i is None or i >= len(row):
                return None
            return (row[i] or "").strip()

        errs: List[str] = []
        warns: List[str] = []

        sku = (val("SKU") or "").upper()
        nombre = val("NOMBRE") or ""
        uom = (val("UNIDAD_MEDIDA") or "").upper()
        clasif = (val("CLASIFICACION") or "").upper()

        if not sku: errs.append("SKU es requerido.")
        if not nombre: errs.append("NOMBRE es requerido.")
        if not uom: errs.append("UNIDAD_MEDIDA es requerida.")
        if clasif not in ("MP", "PT"):
            errs.append("CLASIFICACION debe ser MP o PT.")

        if sku in seen_skus:
            errs.append("SKU duplicado en el archivo.")
        seen_skus.add(sku)

        # ─────────────────────────────────────────────────────────
        # NUEVO: advertir si ese SKU ya existe en la base de datos
        # ─────────────────────────────────────────────────────────
        if sku and sku in existing_skus:
            warns.append(f"SKU '{sku}' ya existe en la base de datos; se actualizará en la confirmación.")
        # ─────────────────────────────────────────────────────────

        pneto = _to_int(val("PRECIO_NETO"), 0)
        if pneto <= 0:
            errs.append("PRECIO_NETO debe ser mayor a 0.")

        dg = val("NOMBRE_GRUPO") or ""
        dsg = val("NOMBRE_SUBGRUPO") or ""
        codigo_g = _to_int(val("CODIGO_GRUPO"), 0)
        codigo_sg = _to_int(val("CODIGO_SUBGRUPO"), 0)

        cod_g_res, cod_sg_res, dg_res, dsg_res = resolve_codes(dg, dsg, codigo_g, codigo_sg)

        if not dg and not codigo_g:
            errs.append("Debe informar NOMBRE_GRUPO o CODIGO_GRUPO.")
        if not dsg and not codigo_sg:
            errs.append("Debe informar NOMBRE_SUBGRUPO o CODIGO_SUBGRUPO.")

        c_barra = _to_int(val("CODIGO_BARRA"), 0)
        valor_repo = _to_int(val("VALOR_REPOSICION"), 0)  # opcional

        payload = {
            "nombre": nombre.upper(),
            "nombre_ci": nombre.lower(),
            "sku": sku,
            "c_barra": c_barra,
            "unidad": uom.upper(),
            "dg": dg_res.upper() or "",
            "codigo_g": int(cod_g_res or 0),
            "dsg": dsg_res.upper() or "",
            "codigo_sg": int(cod_sg_res or 0),
            "pneto": pneto,
            "piva": int(round(pneto * 1.19)),
            "tipo": clasif.upper(),
            "valor_repo": str(valor_repo),
        }

        if errs:
            errorsByRow[r_index] = errs
        if warns:
            warningsByRow[r_index] = warns

        if len(preview) < PREVIEW_LIMIT:
            preview.append({**payload, "__row": r_index})

        items_for_batch.append({
            "row": r_index,
            "payload": payload,
            "errors": errs,
            "warnings": warns,
        })

    # Guardamos batch en Mongo vía repositorio (con TTL)
    batch_id = await products_repo.save_import_batch(db, items_for_batch, ttl_minutes=30)

    columns_out = ["FILA", *headers]
    rows_out = []
    for row in preview:
        r = {"FILA": row.get("__row")}
        r.update({k: row.get(k) for k in headers})
        rows_out.append(r)

    return JSONResponse({
        "batchId": batch_id,
        "columns": [
            "__row",        # índice de fila del CSV
            "sku",
            "c_barra",
            "nombre",
            "unidad",
            "dg",
            "codigo_g",
            "dsg",
            "codigo_sg",
            "pneto",
            "valor_repo",
            "tipo",
        ],
        "rows": preview,
        "errorsByRow": errorsByRow,
        "warningsByRow": warningsByRow,
    })

@router.post("/import/confirm")
async def import_confirm(
    data: Dict[str, Any] = Body(...),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Confirma un batch validado:
    - Lee el batch (repo), filtra filas con error
    - Fuerza activo=True
    - Ejecuta bulk upsert por SKU (repo)
    - Borra el batch
    """
    batch_id = data.get("batchId")
    if not batch_id:
        raise HTTPException(status_code=400, detail="Falta batchId")

    batch = await products_repo.get_import_batch(db, batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="Batch no encontrado o expirado")

    items = batch.get("items", [])
    if not items:
        await products_repo.delete_import_batch(db, batch_id)
        return {"ok": True, "created": 0, "updated": 0, "skipped": 0}

    docs: List[Dict[str, Any]] = []
    skipped = 0
    for it in items:
        if it.get("errors"):
            skipped += 1
            continue
        doc = dict(it["payload"])
        doc["activo"] = True  # regla: todos se crean/habilitan activos
        sku = (doc.get("sku") or "").strip().upper()
        if not sku:
            skipped += 1
            continue
        docs.append(doc)

    try:
        created, updated = await products_repo.bulk_upsert_products_by_sku(db, docs)
    except BulkWriteError as bwe:
        # Caso de conflicto masivo u otros errores del bulk
        raise HTTPException(status_code=400, detail=str(bwe.details))

    # Limpieza del batch
    await products_repo.delete_import_batch(db, batch_id)

    return {"ok": True, "created": created, "updated": updated, "skipped": skipped}
