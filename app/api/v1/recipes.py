# app/api/v1/recipes.py
from __future__ import annotations

import re
from typing import List, Optional, Literal

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel, ConfigDict, Field
from bson import ObjectId
from datetime import datetime , timezone
from fastapi.responses import StreamingResponse
from fastapi.responses import PlainTextResponse
import csv, io

from app.db.mongo import get_db
from app.db.repositories import recipes_repo
from app.services import recipes_service
from app.services import recipes_valuation

# Schemas de entrada (request) para crear/actualizar
from app.models.recipes import (
    CreateRecetaIn,
    RecetaVersionIn,
    UpdateRecetaIn,
    UpdateRecetaVersionIn,
    UpdateComponentesIn,
    UpdateRecetaNombreIn
)

router = APIRouter(prefix="/recipes", tags=["recipes"])

# ----------------------- Modelo de salida flexible ---------------------------
# Nota: Usamos un modelo con extra="allow" para no restringir los campos que
# ya devuelves desde la BD (id, productPTId, versiones, audit, etc).
class RecipeOut(BaseModel):
    id: str
    model_config = ConfigDict(extra="allow")

# --------------------------- Helpers de serialización ------------------------
def bson_to_py(value):
    """Convierte ObjectId a str de manera recursiva para respuestas JSON."""
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, list):
        return [bson_to_py(v) for v in value]
    if isinstance(value, dict):
        return {k: bson_to_py(v) for k, v in value.items()}
    return value

def _to_out_dict(doc: dict) -> dict:
    """Transforma un doc Mongo a un dict API-friendly, renombrando _id -> id."""
    data = bson_to_py(doc)
    if "_id" in data:
        data["id"] = data.pop("_id")
    return data

# ========================= MODELOS PARA VALORIZACIÓN =========================
class ValueRecipeIn(BaseModel):
    """Parámetros de valorización."""
    cost_method: Literal["last", "avg", "std", "pneto", "piva"] = Field(
        "last", description="Método de costo por componente"
    )
    currency: str = Field("CLP", description="Moneda para la valorización (etiqueta)")
class ValueBreakdownItem(BaseModel):
    sku: str
    productId: str
    unit_cost: float
    qty_eff: float
    subtotal: float

class ValueRecipeOut(BaseModel):
    skuPT: str
    version: int
    currency: str
    breakdown: List[ValueBreakdownItem]
    process_cost: float
    total: float
    valued_at: str  # ISO
    warnings: List[str] = []

# -------------------------------- Listado ------------------------------------
@router.get("", response_model=List[RecipeOut])
async def list_recipes(
    q: Optional[str] = Query(
        None,
        description="Filtro simple por nombre (regex, case-insensitive)."
    ),
    limit: int = Query(20, ge=1, le=1000, description="Máximo de items a devolver."),
    skip: int = Query(0, ge=0, description="Desplazamiento para paginado."),
    sort_field: Optional[str] = Query(
        None, description="Campo por el que ordenar (ej: 'nombre', 'id')."
    ),
    sort_dir: Optional[Literal[-1, 1]] = Query(
        None, description="Dirección de orden: 1 asc, -1 desc."
    ),
):
    """
    GET /recipes
    - Construye un filtro simple (por nombre si viene 'q').
    - Llama al repositorio para traer documentos paginados y ordenados.
    - Serializa cada doc para exponer 'id' en lugar de '_id'.
    """
    filtro = {}
    if q:
        # Búsqueda por nombre con regex (case-insensitive)
        filtro = {"nombre": {"$regex": q, "$options": "i"}}

    sort = [(sort_field, sort_dir)] if (sort_field is not None and sort_dir is not None) else None

    docs = await recipes_repo.find_all(
        filtro=filtro,
        limit=limit,
        skip=skip,
        sort=sort,
    )
    return [_to_out_dict(d) for d in docs]

# ----------------------------- Búsqueda like ---------------------------------
@router.get("/like-name/{name}", response_model=List[RecipeOut])
async def get_like_name(
    name: str,
    limit: int = Query(20, ge=1, le=1000, description="Máximo de items a devolver."),
    skip: int = Query(0, ge=0, description="Desplazamiento para paginado."),
):
    """
    GET /recipes/like-name/{name}
    - Búsqueda %name% (case-insensitive) sobre 'nombre_ci'
    """
    docs = await recipes_repo.find_like(
        name,
        limit=limit,
        skip=skip
    )
    if not docs:
        raise HTTPException(status_code=404, detail="Receta(s) no encontradas.")
    return [_to_out_dict(d) for d in docs]

# ----------------------------- Búsqueda mixta --------------------------------
@router.get("/by-mixed/", response_model=List[RecipeOut])
async def find_product_mixed(
    name: str | None = Query(None),
    productPTId: str | None = Query(None),
    estado: str | None = Query(None),
    limit: int = Query(20, ge=1, le=1000, description="Máximo de items a devolver."),
    skip: int = Query(0, ge=0, description="Desplazamiento para paginado."),
):
    """
    GET /recipes/by-mixed
    - Filtro mixto por nombre_ci, productPTId (regex), y estado dentro de versiones
    """
    filtro: dict = {}
    if name:
        filtro["nombre_ci"] = {"$regex": f"{re.escape(name.lower())}"}
    if estado:
        filtro["versiones.estado"] = {"$regex": f"{re.escape(estado.lower())}"}

    docs = await recipes_repo.find_product_mixed(
        filtro,
        limit=limit,
        skip=skip
    )
    if not docs:
        raise HTTPException(status_code=404, detail="Receta(s) no encontradas.")
    return [_to_out_dict(d) for d in docs]

# --------------------------- Crear receta (PT) --------------------------------
@router.post("", response_model=RecipeOut)
async def create_recipe(body: CreateRecetaIn, db=Depends(get_db)):
    """
    POST /recipes
    Crea una receta para un PT con su primera versión.
    - Resuelve SKUs → ObjectId
    - Proceso opcional (catálogo o especial)
    - Defaults: fechaPublicacion=hoy (si no viene), mermaPct=0
    """
    try:
        doc = await recipes_service.create_recipe(db, body)
        return doc  # ya viene con ids stringeados desde el service
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

# --------------------------- Agregar nueva versión ---------------------------
@router.post("/{skuPT}/versions", response_model=RecipeOut)
async def add_version(skuPT: str, body: RecetaVersionIn, db=Depends(get_db)):
    """
    POST /recipes/{skuPT}/versions
    Agrega una NUEVA versión a la receta existente del PT.
    """
    try:
        doc = await recipes_service.add_version(db, skuPT, body)
        return doc
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

# --------------------------- Marcar versión vigente --------------------------
@router.put("/{skuPT}", response_model=RecipeOut)
async def set_vigente(skuPT: str, body: UpdateRecetaIn, db=Depends(get_db)):
    """
    PUT /recipes/{skuPT}
    Actualiza metadatos: setear 'vigenteVersion' del documento raíz.
    """
    if body.vigenteVersion is None:
        raise HTTPException(status_code=422, detail="vigenteVersion requerido")
    try:
        doc = await recipes_service.set_vigente(db, skuPT, int(body.vigenteVersion))
        return doc
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

# --------------------------- Update versión completa -------------------------
@router.put("/{skuPT}/versions/{version}", response_model=RecipeOut)
async def update_version_full(
    skuPT: str,
    version: int,
    body: UpdateRecetaVersionIn,
    db=Depends(get_db),
):
    """
    PUT /recipes/{skuPT}/versions/{version}
    Actualiza campos de una versión específica. Si envías 'componentes',
    se reemplaza la lista completa (agrupa y suma duplicados por MP).
    """
    try:
        doc = await recipes_service.update_version_full(db, skuPT, version, body)
        return doc
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

# --------------------------- Reemplazar componentes --------------------------
@router.patch("/{skuPT}/versions/{version}/componentes", response_model=RecipeOut)
async def replace_componentes(
    skuPT: str,
    version: int,
    body: UpdateComponentesIn,
    db=Depends(get_db),
):
    """
    PATCH /recipes/{skuPT}/versions/{version}/componentes
    Reemplaza COMPLETAMENTE los componentes de una versión (BOM).
    """
    try:
        doc = await recipes_service.replace_componentes(db, skuPT, version, body)
        return doc
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

# ============================ VALORIZACIÓN (NUEVO) ===========================
@router.post("/{skuPT}/versions/{version}/valorizar:preview", response_model=ValueRecipeOut)
async def value_version_preview(
    skuPT: str,
    version: int,
    body: ValueRecipeIn,
    db=Depends(get_db),
):
    """
    POST /recipes/{skuPT}/versions/{version}/valorizar:preview
    - Calcula costo SIN persistir en BD.
    - Usa products.cost por 'cost_method' (last|avg|std).
    - Aplica merma sobre cantidades de componentes.
    - Suma costo de proceso (processId o procesoEspecial_costo).
    """
    try:
        result = await recipes_valuation.value_version(db, skuPT, version, body.cost_method, body.currency, persist=False)
        return ValueRecipeOut(**result)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

@router.post("/{skuPT}/versions/{version}/valorizar", response_model=ValueRecipeOut)
async def value_version_persist(
    skuPT: str,
    version: int,
    body: ValueRecipeIn,
    db=Depends(get_db),
):
    """
    POST /recipes/{skuPT}/versions/{version}/valorizar
    - Calcula y PERSISTE el costo en la versión (campo 'versiones.$.cost').
    """
    try:
        result = await recipes_valuation.value_version(db, skuPT, version, "pneto", body.currency, persist=True)
        return ValueRecipeOut(**result)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

# ------------------------ Obtener receta por productPTId ---------------------
@router.get("/by-pt-id/{pt_ref}", response_model=RecipeOut)
async def get_recipe_by_pt_id(pt_ref: str, db=Depends(get_db)):
    """
    GET /recipes/by-pt-id/{pt_ref}
    Permite buscar la receta usando el ObjectId del PT o directamente su SKU.
    """
    try:
        doc = await recipes_service.get_recipe_by_pt_id(db, pt_ref)
        return doc
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

# --------------------------- Obtener receta por SKU PT -----------------------
# IMPORTANTE: este endpoint debe ir al final (para no interferir con rutas anteriores).
@router.get("/{skuPT}", response_model=RecipeOut)
async def get_recipe_by_sku(skuPT: str, db=Depends(get_db)):
    """
    GET /recipes/{skuPT}
    Obtiene la receta (todas sus versiones) por SKU del Producto Terminado.
    """
    try:
        doc = await recipes_service.get_recipe_by_sku(db, skuPT)
        return doc
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

# ===================== HABILITAR / DESHABILITAR VERSIONES ====================

class EnableIn(BaseModel):
    version: int = Field(..., ge=1, description="Número de versión a habilitar")

@router.post("/{skuPT}/enable", response_model=RecipeOut)
async def enable_recipe(skuPT: str, body: EnableIn, db=Depends(get_db)):
    """
    Fija la versión indicada como 'vigente'.
    - Marca 'vigente' a esa versión
    - Si había una vigente distinta, la marca 'obsoleta'
    - Actualiza 'vigenteVersion'
    """
    try:
        doc = await recipes_service.set_vigente(db, skuPT, int(body.version))
        return doc
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

@router.post("/{skuPT}/disable", response_model=RecipeOut)
async def disable_current_vigente(skuPT: str, db=Depends(get_db)):
    """
    Deshabilita la versión vigente actual del PT:
    - Si hay 'vigenteVersion', la marca 'obsoleta'
    - Limpia 'vigenteVersion' (unset)
    """
    # Traer receta por skuPT
    try:
        rec = await recipes_service.get_recipe_by_sku(db, skuPT)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    vigente = rec.get("vigenteVersion")
    if vigente is None:
        # Idempotente: nada que deshabilitar
        return rec

    # Marcar vigente como obsoleta
    updated = await recipes_repo.update_version_estado(
        recipe_id=ObjectId(rec["id"]),
        version_num=int(vigente),
        nuevo_estado="obsoleta",
        db=db,
    )
    # Unset de vigenteVersion
    cleaned = await recipes_repo.clear_vigente_version(
        recipe_id=ObjectId(rec["id"]),
        db=db,
    )
    return _to_out_dict(cleaned)

@router.post("/{skuPT}/versions/{version}/enable", response_model=RecipeOut)
async def enable_specific_version(skuPT: str, version: int, db=Depends(get_db)):
    """
    Habilita específicamente la versión indicada (alias de set_vigente).
    """
    try:
        doc = await recipes_service.set_vigente(db, skuPT, int(version))
        return doc
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

@router.post("/{skuPT}/versions/{version}/disable", response_model=RecipeOut)
async def disable_specific_version(skuPT: str, version: int, db=Depends(get_db)):
    """
    Deshabilita (marca 'obsoleta') la versión indicada. Si era la vigente, también limpia 'vigenteVersion'.
    """
    # Traer receta por skuPT
    try:
        rec = await recipes_service.get_recipe_by_sku(db, skuPT)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    versiones = rec.get("versiones", [])
    if not any(int(v.get("version")) == int(version) for v in versiones):
        raise HTTPException(status_code=422, detail=f"La versión {version} no existe para este PT")

    # Marcar la versión como obsoleta
    await recipes_repo.update_version_estado(
        recipe_id=ObjectId(rec["id"]),
        version_num=int(version),
        nuevo_estado="obsoleta",
        db=db,
    )

    # Si era la vigente, limpiar vigenteVersion
    if rec.get("vigenteVersion") is not None and int(rec["vigenteVersion"]) == int(version):
        cleaned = await recipes_repo.clear_vigente_version(
            recipe_id=ObjectId(rec["id"]),
            db=db,
        )
        return _to_out_dict(cleaned)

    # Si no era la vigente, devolver doc actualizado
    updated = await recipes_repo.find_by_id(rec["id"], db=db)
    return _to_out_dict(updated)

# --- IMPORT CSV ---
from fastapi import UploadFile, File, Form
import csv, io, uuid

class ImportStageOut(BaseModel):
    batch_id: str
    inserted: int
    warnings: List[str] = []

@router.post("/import/csv:stage", response_model=ImportStageOut)
async def import_csv_stage(
    file: UploadFile = File(...),
    db=Depends(get_db),
):
    """
    Sube CSV a 'staging_recipes' con un batch_id.
    Columnas esperadas (cabeceras): sku_PT,version,estado,marcar_vigente,base_qty,unidad_PT,
      sku_MP,cantidad_por_base,unidad_MP,merma_pct,process_codigo,process_especial_nombre,
      process_especial_costo,fecha_publicacion,publicado_por,notas
    """
    content = await file.read()
    text = content.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))
    rows = [r for r in reader]

    if not rows:
        raise HTTPException(status_code=422, detail="CSV vacío")

    batch_id = str(uuid.uuid4())
    ins, warnings = await recipes_repo.stage_insert_rows(rows, batch_id=batch_id, db=db)
    return ImportStageOut(batch_id=batch_id, inserted=ins, warnings=warnings)

class ImportStatusOut(BaseModel):
    batch_id: str
    total: int
    first_rows: List[dict]

@router.get("/import/csv:status", response_model=ImportStatusOut)
async def import_csv_status(batch_id: str = Query(...), db=Depends(get_db)):
    total, sample = await recipes_repo.stage_status(batch_id=batch_id, db=db)
    return ImportStatusOut(batch_id=batch_id, total=total, first_rows=sample)

class PromoteIn(BaseModel):
    batch_id: str
    overwrite_version: bool = False
    dry_run: bool = False

class PromoteOut(BaseModel):
    gruposProcesados: int
    recetasCreadas: int
    recetasActualizadas: int
    versionesAgregadas: int
    versionesRechazadas: int
    vigentesSeteadas: int
    warnings: List[str]
    errores: List[str]

@router.post("/import/csv:promote", response_model=PromoteOut)
async def import_csv_promote(body: PromoteIn, db=Depends(get_db)):
    res = await recipes_service.promote_staging_batch(
        db=db,
        batch_id=body.batch_id,
        overwrite_version=body.overwrite_version,
        dry_run=body.dry_run,
    )
    return PromoteOut(**res)

@router.delete("/import/csv:stage/{batch_id}")
async def import_csv_clear(batch_id: str, db=Depends(get_db)):
    deleted = await recipes_repo.stage_clear(batch_id=batch_id, db=db)
    return {"batch_id": batch_id, "deleted": deleted}

@router.get("/import/csv:template")
async def download_recipes_csv_template(sample: bool = False):
    """
    Devuelve la plantilla CSV para importar recetas.
    - ?sample=true incluye 2-3 filas de ejemplo
    """
    # Encabezados EXACTOS (coinciden con staging)
    headers = [
        "sku_PT","version","estado","marcar_vigente","base_qty","unidad_PT",
        "sku_MP","cantidad_por_base","unidad_MP","merma_pct",
        "process_codigo","process_especial_nombre","process_especial_costo",
        "fecha_publicacion","publicado_por","notas"
    ]

    # Buffer en memoria
    buffer = io.StringIO()
    writer = csv.writer(buffer)

    # Escribir cabeceras
    writer.writerow(headers)

    if sample:
        # Filas de ejemplo (una fila por componente)
        writer.writerow(["301002", 1, "borrador", "true", 1, "un", "801007", 10, "kg", 2, "PROC-ABC", "", "", "2025-10-13", "import_csv", "Receta base v1"])
        writer.writerow(["301002", 1, "borrador", "true", 1, "un", "901576", 1, "un", 0, "PROC-ABC", "", "", "2025-10-13", "import_csv", ""])
        writer.writerow(["500100", 1, "borrador", "false", 10, "kg", "702010", 3.5, "kg", 1, "", "mezcla fina", 1200, "2025-10-14", "import_csv", "Proceso especial"])

    buffer.seek(0)

    # Nombre de archivo
    filename = "plantilla_recetas.csv" if not sample else "plantilla_recetas_ejemplo.csv"

    return StreamingResponse(
        buffer,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        },
    )

#========================== Manual ===============================
def _manual_text() -> str:
    return """MANUAL DE USO – Importación de Recetas (CSV)

¿PARA QUÉ SIRVE?
Para crear o actualizar recetas de productos cargando un archivo CSV (plantilla).

ANTES DE EMPEZAR
- Ten los códigos del Producto Terminado (sku_PT) y de cada insumo (sku_MP).
- Descarga la plantilla desde “Descargar plantilla CSV”.

CÓMO COMPLETAR LA PLANTILLA
Cada fila representa un COMPONENTE (insumo) de una VERSIÓN de receta.

OBLIGATORIOS POR FILA
- sku_PT               : código de producto terminado (ej: 301002)
- version              : entero > 0 (ej: 1)
- sku_MP               : código del insumo/componente
- cantidad_por_base    : número > 0 (cantidad del insumo para la receta base)

OPCIONALES (con valores por defecto)
- estado               : borrador | vigente | obsoleta   (default: borrador)
   • borrador  : creada, aún no se usa; editable
   • vigente   : activa en producción (solo una por PT)
   • obsoleta  : histórica, no se usa para producir
- marcar_vigente       : true/false (default: false) → si true, esa versión queda activa
- base_qty             : rendimiento base (default: 1)
- unidad_PT / unidad_MP: unidades (ej: un, kg)
- merma_pct            : 0–100 (default: 0)
- process_codigo       : código de proceso estándar  (O usar los campos especiales)
- process_especial_nombre, process_especial_costo: si NO usas process_codigo
- fecha_publicacion, publicado_por, notas: informativos

NOTAS
- Si repites el mismo sku_MP en la misma versión, el sistema SUMA cantidades.
- Si el mismo sku_MP trae unidades distintas, se conserva la PRIMERA (se avisará).

PASO A PASO
1) Completa la plantilla y guarda como .csv
2) Sube el archivo (botón “Subir CSV”)
3) Revisa el resumen (cantidad de filas y muestra)
4) Promueve para guardar:
   - Puedes “Simular” (no guarda) y luego “Promover”
5) (Opcional) Limpia el lote de carga

PREGUNTAS RÁPIDAS
- ¿Se puede usar una receta en ‘borrador’? → No. Debes habilitarla (botón “Habilitar versión”)
- ¿Qué versión queda activa si cargo varias? → La que tenga marcar_vigente=true (solo una por PT)
- ¿Costos con IVA? → El sistema guarda NETO; para ver con IVA se aplica la tasa correspondiente.

EJEMPLO MÍNIMO (3 filas, misma receta v1)
sku_PT,version,estado,marcar_vigente,base_qty,unidad_PT,sku_MP,cantidad_por_base,unidad_MP,merma_pct,process_codigo,process_especial_nombre,process_especial_costo,fecha_publicacion,publicado_por,notas
301002,1,borrador,true,1,un,801007,10,kg,2,PROC-ABC,,,2025-10-13,import_csv,Receta base
301002,1,borrador,true,1,un,901576,1,un,0,PROC-ABC,,,2025-10-13,import_csv,
301002,1,borrador,true,1,un,901577,1,un,0,PROC-ABC,,,2025-10-13,import_csv,
"""

@router.get("/import/csv:manual", response_class=PlainTextResponse)
async def download_recipes_csv_manual():
    """
    Manual de uso para la plantilla CSV (TXT plano, sin dependencias).
    """
    return PlainTextResponse(
        content=_manual_text(),
        headers={"Content-Disposition": 'attachment; filename="manual_csv_recetas.txt"'}
    )

#====== Metodo para modificar nombre de receta =======
@router.patch("/{skuPT}", summary="Actualizar nombre de la receta (PT) por SKU")
async def update_recipe_name_by_sku(
    skuPT: str,
    body: UpdateRecetaNombreIn,
    db=Depends(get_db)
):
    modified = await recipes_repo.set_recipe_name_by_sku(skuPT, body.nombre, db=db)
    if modified == 0:
        # coherente con el resto: 404 si no existe PT/receta para ese SKU
        raise HTTPException(status_code=404, detail="Receta o PT no encontrado para el SKU indicado")

    # respuesta simple; el front después hace getById(sku) para refrescar
    return {"ok": True, "skuPT": skuPT, "nombre": body.nombre}
