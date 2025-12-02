# app/services/recipes_service.py
from __future__ import annotations

from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, date
from bson import ObjectId

from app.db.repositories import recipes_repo

# ------------------------ helpers de mapeo/fechas ----------------------------
def _oid_str(oid: ObjectId | None) -> Optional[str]:
    return str(oid) if oid is not None else None

def _today_utc_date_only() -> date:
    now = datetime.now(timezone.utc)
    return date(year=now.year, month=now.month, day=now.day)

def _normalize_publication_datetime(value: Any) -> datetime:
    """
    Acepta date/datetime/str y devuelve datetime timezone-aware en UTC.
    """
    if value is None:
        value = _today_utc_date_only()

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return _normalize_publication_datetime(None)
        try:
            parsed_date = date.fromisoformat(text)
        except ValueError:
            iso_text = text.replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(iso_text)
            except ValueError:
                for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
                    try:
                        dt = datetime.strptime(text, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    raise ValueError(f"fechaPublicacion inválida: '{value}'")
        else:
            return _normalize_publication_datetime(parsed_date)
    else:
        raise ValueError(f"fechaPublicacion inválida: '{value}'")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt

def _map_recipe_out(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Convierte ObjectIds a str y expone 'id' en lugar de '_id'."""
    def map_version(v: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(v)
        if "processId" in out and out["processId"] is not None:
            out["processId"] = _oid_str(out["processId"])
        out["componentes"] = [
            {
                "productId": _oid_str(c["productId"]),
                "cantidadPorBase": c["cantidadPorBase"],
                "unidad": c["unidad"],
                "merma_pct": c.get("merma_pct", 0.0),
            }
            for c in v.get("componentes", [])
        ]
        return out

    return {
        "id": _oid_str(doc["_id"]),
        "productPTId": _oid_str(doc["productPTId"]),
        "vigenteVersion": doc.get("vigenteVersion"),
        "versiones": [map_version(v) for v in doc.get("versiones", [])],
        "audit": {
            "createdAt": doc["audit"]["createdAt"],
            "updatedAt": doc["audit"]["updatedAt"],
        },
    }

# ------------------------ reglas de negocio ----------------------------------
async def create_recipe(db, payload) -> Dict[str, Any]:
    """Crea receta para un PT y agrega su primera versión (payload = CreateRecetaIn)."""
    pt = await recipes_repo.get_pt_by_sku(payload.skuPT, db)
    if not pt:
        raise ValueError(f"PT no encontrado para skuPT={payload.skuPT}")

    existing = await recipes_repo.find_by_pt_id(pt["_id"], db=db)
    if existing:
        raise ValueError("La receta para este PT ya existe. Use agregar versión.")

    # Base versión
    v = payload.version
    try:
        fecha_pub = _normalize_publication_datetime(v.fechaPublicacion)
    except ValueError as exc:
        raise ValueError(str(exc))
    version_doc: Dict[str, Any] = {
        "version": v.numero,
        "estado": v.estado,
        "fechaPublicacion": fecha_pub,
        "publicadoPor": v.publicadoPor,
        "base_qty": v.baseQty,
        "unidad_PT": v.unidadPT,
        "componentes": [],
    }

    # Proceso (opcional)
    if v.proceso:
        if v.proceso.processCodigo:
            pr = await recipes_repo.get_process_by_code(v.proceso.processCodigo, db)
            if pr:
                version_doc["processId"] = pr["_id"]
        else:
            if v.proceso.procesoEspecialNombre:
                version_doc["procesoEspecial_nombre"] = v.proceso.procesoEspecialNombre
            if v.proceso.procesoEspecialCosto is not None:
                version_doc["procesoEspecial_costo"] = v.proceso.procesoEspecialCosto

    # Componentes (agrupar por productId)
    agg: Dict[str, Dict[str, Any]] = {}
    for c in v.componentes:
        mp = await recipes_repo.get_product_by_sku(c.skuMP, db)
        if not mp:
            raise ValueError(f"Componente skuMP={c.skuMP} no existe")
        key = str(mp["_id"])
        item = agg.get(
            key,
            {
                "productId": mp["_id"],
                "cantidadPorBase": 0.0,
                "unidad": c.unidad,
                "merma_pct": float(c.mermaPct or 0.0),
            },
        )
        item["cantidadPorBase"] += float(c.cantidadPorBase)
        agg[key] = item
    version_doc["componentes"] = list(agg.values())

    now = datetime.now(timezone.utc)
    receta_doc = {
        "productPTId": pt["_id"],
        "vigenteVersion": payload.vigenteVersion
        if payload.vigenteVersion is not None
        else (v.numero if v.estado == "vigente" and v.marcarVigente else None),
        "versiones": [version_doc],
        "audit": {"createdAt": now, "updatedAt": now, "createdBy": None},
    }

    created = await recipes_repo.insert_recipe(receta_doc, db=db)
    return _map_recipe_out(created)

async def add_version(db, skuPT: str, version_payload) -> Dict[str, Any]:
    """Agrega nueva versión a receta existente (version_payload = RecetaVersionIn)."""
    pt = await recipes_repo.get_pt_by_sku(skuPT, db)
    if not pt:
        raise ValueError(f"PT no encontrado para skuPT={skuPT}")

    rec = await recipes_repo.find_by_pt_id(pt["_id"], db=db)
    if not rec:
        raise ValueError("Receta no existe. Crea la receta primero.")

    # No duplicar versión
    if any(int(v.get("version")) == int(version_payload.numero) for v in rec.get("versiones", [])):
        raise ValueError(f"La versión {version_payload.numero} ya existe")

    try:
        fecha_pub = _normalize_publication_datetime(version_payload.fechaPublicacion)
    except ValueError as exc:
        raise ValueError(str(exc))
    vdoc: Dict[str, Any] = {
        "version": version_payload.numero,
        "estado": version_payload.estado,
        "fechaPublicacion": fecha_pub,
        "publicadoPor": version_payload.publicadoPor,
        "base_qty": version_payload.baseQty,
        "unidad_PT": version_payload.unidadPT,
        "componentes": [],
    }

    # Proceso opcional
    if version_payload.proceso and version_payload.proceso.processCodigo:
        pr = await recipes_repo.get_process_by_code(version_payload.proceso.processCodigo, db)
        if pr:
            vdoc["processId"] = pr["_id"]
    else:
        pe = version_payload.proceso
        if pe:
            if pe.procesoEspecialNombre:
                vdoc["procesoEspecial_nombre"] = pe.procesoEspecialNombre
            if pe.procesoEspecialCosto is not None:
                vdoc["procesoEspecial_costo"] = pe.procesoEspecialCosto

    # Componentes
    agg: Dict[str, Dict[str, Any]] = {}
    for c in version_payload.componentes:
        mp = await recipes_repo.get_product_by_sku(c.skuMP, db)
        if not mp:
            raise ValueError(f"Componente skuMP={c.skuMP} no existe")
        key = str(mp["_id"])
        item = agg.get(
            key,
            {
                "productId": mp["_id"],
                "cantidadPorBase": 0.0,
                "unidad": c.unidad,
                "merma_pct": float(c.mermaPct or 0.0),
            },
        )
        item["cantidadPorBase"] += float(c.cantidadPorBase)
        agg[key] = item
    vdoc["componentes"] = list(agg.values())

    updated = await recipes_repo.push_recipe_version(
        rec["_id"],
        vdoc,
        marcar_vigente=bool(version_payload.marcarVigente),
        updated_at=datetime.now(timezone.utc),
        db=db,
    )
    return _map_recipe_out(updated)

async def set_vigente(db, skuPT: str, version_num: int) -> Dict[str, Any]:
    pt = await recipes_repo.get_pt_by_sku(skuPT, db)
    if not pt:
        raise ValueError("PT no encontrado")
    rec = await recipes_repo.find_by_pt_id(pt["_id"], db=db)
    if not rec:
        raise ValueError("Receta no encontrada")
    if not any(int(v.get("version")) == int(version_num) for v in rec.get("versiones", [])):
        raise ValueError(f"La versión {version_num} no existe para este PT")

    updated = await recipes_repo.set_recipe_meta(
        rec["_id"],
        vigente_version=int(version_num),
        updated_at=datetime.now(timezone.utc),
        db=db,
    )
    return _map_recipe_out(updated)

async def update_version_full(db, skuPT: str, version_num: int, body) -> Dict[str, Any]:
    pt = await recipes_repo.get_pt_by_sku(skuPT, db)
    if not pt:
        raise ValueError("PT no encontrado")
    rec = await recipes_repo.find_by_pt_id(pt["_id"], db=db)
    if not rec:
        raise ValueError("Receta no encontrada")

    idx = next((i for i, v in enumerate(rec.get("versiones", [])) if int(v.get("version")) == int(version_num)), -1)
    if idx < 0:
        raise ValueError(f"Versión {version_num} no existe")

    base = f"versiones.{idx}"
    set_fields: Dict[str, Any] = {}

    if body.estado is not None:
        set_fields[f"{base}.estado"] = body.estado
    if body.fechaPublicacion is not None:
        try:
            set_fields[f"{base}.fechaPublicacion"] = _normalize_publication_datetime(body.fechaPublicacion)
        except ValueError as exc:
            raise ValueError(str(exc))
    if body.publicadoPor is not None:
        set_fields[f"{base}.publicadoPor"] = body.publicadoPor
    if body.baseQty is not None:
        set_fields[f"{base}.base_qty"] = float(body.baseQty)
    if body.unidadPT is not None:
        set_fields[f"{base}.unidad_PT"] = body.unidadPT

    # Proceso
    if body.proceso is not None:
        codigo = getattr(body.proceso, "processCodigo", None)
        if codigo:
            pr = await recipes_repo.get_process_by_code(codigo, db)
            if pr is None:
                raise ValueError(f"Proceso no encontrado para processCodigo={codigo}")
            set_fields[f"{base}.processId"] = pr["_id"]
            set_fields[f"{base}.procesoEspecial_nombre"] = None
            set_fields[f"{base}.procesoEspecial_costo"] = None
        else:
            set_fields[f"{base}.processId"] = None
            set_fields[f"{base}.procesoEspecial_nombre"] = getattr(body.proceso, "procesoEspecialNombre", None)
            set_fields[f"{base}.procesoEspecial_costo"] = getattr(body.proceso, "procesoEspecialCosto", None)

    # Componentes (reemplazo si vienen)
    if body.componentes is not None:
        agg: Dict[str, Dict[str, Any]] = {}
        for c in body.componentes:
            sku = c["skuMP"] if isinstance(c, dict) else c.skuMP
            unidad = c["unidad"] if isinstance(c, dict) else c.unidad
            merma = float((c.get("mermaPct") if isinstance(c, dict) else getattr(c, "mermaPct", 0)) or 0.0)
            qty = float(c["cantidadPorBase"] if isinstance(c, dict) else c.cantidadPorBase)

            mp = await recipes_repo.get_product_by_sku(sku, db)
            if not mp:
                raise ValueError(f"Componente skuMP={sku} no existe")
            key = str(mp["_id"])
            item = agg.get(key, {"productId": mp["_id"], "cantidadPorBase": 0.0, "unidad": unidad, "merma_pct": merma})
            item["cantidadPorBase"] += qty
            agg[key] = item
        set_fields[f"{base}.componentes"] = list(agg.values())

    set_fields["audit.updatedAt"] = datetime.now(timezone.utc)
    updated = await recipes_repo.update_version_fields(rec["_id"], version_num, set_fields, db=db)
    return _map_recipe_out(updated)

async def replace_componentes(db, skuPT: str, version_num: int, body) -> Dict[str, Any]:
    pt = await recipes_repo.get_pt_by_sku(skuPT, db)
    if not pt:
        raise ValueError("PT no encontrado")
    rec = await recipes_repo.find_by_pt_id(pt["_id"], db=db)
    if not rec:
        raise ValueError("Receta no encontrada")

    idx = next((i for i, v in enumerate(rec.get("versiones", [])) if int(v.get("version")) == int(version_num)), -1)
    if idx < 0:
        raise ValueError(f"Versión {version_num} no existe")

    agg: Dict[str, Dict[str, Any]] = {}
    for c in body.componentes:
        sku = c["skuMP"] if isinstance(c, dict) else c.skuMP
        unidad = c["unidad"] if isinstance(c, dict) else c.unidad
        merma = float((c.get("mermaPct") if isinstance(c, dict) else getattr(c, "mermaPct", 0)) or 0.0)
        qty = float(c["cantidadPorBase"] if isinstance(c, dict) else c.cantidadPorBase)

        mp = await recipes_repo.get_product_by_sku(sku, db)
        if not mp:
            raise ValueError(f"Componente skuMP={sku} no existe")
        key = str(mp["_id"])
        item = agg.get(key, {"productId": mp["_id"], "cantidadPorBase": 0.0, "unidad": unidad, "merma_pct": merma})
        item["cantidadPorBase"] += qty
        agg[key] = item

    updated = await recipes_repo.replace_version_components(
        rec["_id"],
        version_num,
        list(agg.values()),
        updated_at=datetime.now(timezone.utc),
        db=db,
    )
    return _map_recipe_out(updated)

async def get_recipe_by_sku(db, skuPT: str) -> Dict[str, Any]:
    pt = await recipes_repo.get_pt_by_sku(skuPT, db)
    if not pt:
        raise ValueError("PT no encontrado")
    rec = await recipes_repo.find_by_pt_id(pt["_id"], db=db)
    if not rec:
        raise ValueError("Receta no encontrada")
    return _map_recipe_out(rec)


async def get_recipe_by_pt_id(db, pt_id_or_sku: str) -> Dict[str, Any]:
    """
    Obtiene receta usando el ObjectId del PT o, si no es válido, resolviendo por sku.
    """
    resolved_pt_id: ObjectId | None = None
    if ObjectId.is_valid(pt_id_or_sku):
        resolved_pt_id = ObjectId(pt_id_or_sku)
    else:
        pt = await recipes_repo.get_pt_by_sku(pt_id_or_sku, db)
        if not pt:
            raise ValueError("PT no encontrado")
        resolved_pt_id = pt["_id"]

    rec = await recipes_repo.find_by_pt_id(resolved_pt_id, db=db)
    if not rec:
        raise ValueError("Receta no encontrada")
    return _map_recipe_out(rec)

async def set_vigente(db, skuPT: str, version_num: int) -> Dict[str, Any]:
    """
    Marca como VIGENTE la versión indicada y, si existe una vigente previa distinta,
    la marca como OBSOLETA. Además actualiza 'vigenteVersion' del documento raíz.
    """
    # 1) Resolver PT y receta
    pt = await recipes_repo.get_pt_by_sku(skuPT, db)
    if not pt:
        raise ValueError("PT no encontrado")

    rec = await recipes_repo.find_by_pt_id(pt["_id"], db=db)
    if not rec:
        raise ValueError("Receta no encontrada")

    versiones = rec.get("versiones", [])
    if not any(int(v.get("version")) == int(version_num) for v in versiones):
        raise ValueError(f"La versión {version_num} no existe para este PT")

    prev_vig = rec.get("vigenteVersion")
    now = datetime.now(timezone.utc)

    # 2) Marcar target como 'vigente'
    await recipes_repo.update_version_estado(
        recipe_id=rec["_id"],
        version_num=int(version_num),
        nuevo_estado="vigente",
        db=db,
    )

    # 3) Si hay vigente anterior y es distinta, marcarla 'obsoleta'
    if prev_vig is not None and int(prev_vig) != int(version_num):
        await recipes_repo.update_version_estado(
            recipe_id=rec["_id"],
            version_num=int(prev_vig),
            nuevo_estado="obsoleta",
            db=db,
        )

    # 4) Actualizar meta 'vigenteVersion' y 'audit.updatedAt'
    updated = await recipes_repo.set_recipe_meta(
        recipe_id=rec["_id"],
        vigente_version=int(version_num),
        updated_at=now,
        db=db,
    )

    return _map_recipe_out(updated)

from math import isfinite

def _to_bool(v) -> bool:
    if isinstance(v, bool): return v
    s = str(v).strip().lower()
    return s in {"true","1","si","sí","y","yes"}

def _to_num(v, default=0.0) -> float:
    try:
        n = float(v)
        return n if isfinite(n) else default
    except Exception:
        return default

async def promote_staging_batch(db, batch_id: str, *, overwrite_version: bool = False, dry_run: bool = False) -> Dict[str, Any]:
    STAGING = await recipes_repo.staging_coll(db)
    PRODUCTS = await recipes_repo.products_coll(db)
    PROCESSES = await recipes_repo.processes_coll(db)
    RECIPES = await recipes_repo.get_collection(db)

    warnings: List[str] = []
    errores: List[str] = []

    # 1) Traer filas del batch y agrupar por (sku_PT, version)
    rows = [r async for r in STAGING.find({"batch_id": batch_id})]
    grupos: Dict[tuple, List[Dict[str, Any]]] = {}
    for r in rows:
        key = (str(r.get("sku_PT") or "").strip(), int(_to_num(r.get("version"), 0)))
        if not key[0] or key[1] <= 0:
            warnings.append(f"Fila inválida (sku_PT/version): {r}")
            continue
        grupos.setdefault(key, []).append(r)

    res = {
        "gruposProcesados": len(grupos),
        "recetasCreadas": 0,
        "recetasActualizadas": 0,
        "versionesAgregadas": 0,
        "versionesRechazadas": 0,
        "vigentesSeteadas": 0,
        "warnings": warnings,
        "errores": errores,
    }

    for (skuPT, versionNum), rows_g in grupos.items():
        # PT
        pt = await PRODUCTS.find_one({"sku": skuPT, "tipo": "PT"})
        if not pt:
            errores.append(f"PT no encontrado para sku_PT='{skuPT}'")
            continue

        # Cabecera (primer valor no vacío)
        def first(key): 
            for rr in rows_g:
                v = rr.get(key)
                if v is not None and str(v).strip() != "": return v
            return None

        estado = (first("estado") or "borrador").strip().lower()
        marcar_vigente = _to_bool(first("marcar_vigente"))
        base_qty = _to_num(first("base_qty"), 1)
        unidad_PT = first("unidad_PT")
        publicado_por = first("publicado_por")
        raw_fecha_publicacion = first("fecha_publicacion")
        try:
            fecha_publicacion = _normalize_publication_datetime(raw_fecha_publicacion)
        except ValueError:
            errores.append(
                f"fecha_publicacion inválida '{raw_fecha_publicacion}' (sku_PT='{skuPT}', v={versionNum})"
            )
            continue

        # Proceso
        process_codigo = first("process_codigo")
        processId = None
        if process_codigo:
            pr = await PROCESSES.find_one({"codigo": str(process_codigo).strip()})
            if pr: processId = pr["_id"]
            else: warnings.append(f"process_codigo='{process_codigo}' no encontrado (sku_PT='{skuPT}', v={versionNum})")
        procesoEspecial_nombre = first("process_especial_nombre")
        procesoEspecial_costo = first("process_especial_costo")
        procesoEspecial_costo = None if (procesoEspecial_costo is None or str(procesoEspecial_costo).strip()=="") else _to_num(procesoEspecial_costo, None)

        # Componentes (agrupar por sku_MP)
        comp_map: Dict[str, Dict[str, Any]] = {}
        for rr in rows_g:
            skuMP = str(rr.get("sku_MP") or "").strip()
            if not skuMP:
                warnings.append(f"Fila sin sku_MP (sku_PT='{skuPT}', v={versionNum})")
                continue
            qty = _to_num(rr.get("cantidad_por_base"), 0)
            unidad_MP = (rr.get("unidad_MP") or None)
            merma = _to_num(rr.get("merma_pct"), 0)
            item = comp_map.get(skuMP) or {"cantidad": 0.0, "unidad_MP": unidad_MP, "merma_pct": merma}
            item["cantidad"] += qty
            comp_map[skuMP] = item

        componentes: List[Dict[str, Any]] = []
        for skuMP, info in comp_map.items():
            mp = await PRODUCTS.find_one({"sku": skuMP})
            if not mp:
                errores.append(f"MP no encontrado: sku_MP='{skuMP}' (sku_PT='{skuPT}', v={versionNum})")
                continue
            componentes.append({
                "productId": mp["_id"],
                "cantidadPorBase": info["cantidad"],
                "unidad": info["unidad_MP"],
                "merma_pct": info.get("merma_pct", 0),
            })

        if not componentes:
            warnings.append(f"Sin componentes válidos (sku_PT='{skuPT}', v={versionNum})")
            continue

        version_doc: Dict[str, Any] = {
            "version": int(versionNum),
            "estado": estado,
            "fechaPublicacion": fecha_publicacion,
            "publicadoPor": publicado_por,
            "base_qty": base_qty,
            "unidad_PT": unidad_PT,
            "componentes": componentes,
        }
        if processId:
            version_doc["processId"] = processId
        else:
            if procesoEspecial_nombre: version_doc["procesoEspecial_nombre"] = procesoEspecial_nombre
            if procesoEspecial_costo is not None: version_doc["procesoEspecial_costo"] = procesoEspecial_costo

        now = datetime.now(timezone.utc)
        existing = await RECIPES.find_one({"productPTId": pt["_id"]})

        if not existing:
            if dry_run:
                res["recetasCreadas"] += 1
                if marcar_vigente: res["vigentesSeteadas"] += 1
            else:
                new_doc = {
                    "productPTId": pt["_id"],
                    "vigenteVersion": int(versionNum) if marcar_vigente else None,
                    "versiones": [version_doc],
                    "audit": {"createdAt": now, "updatedAt": now, "createdBy": None},
                }
                await RECIPES.insert_one(new_doc)
                res["recetasCreadas"] += 1
                if marcar_vigente: res["vigentesSeteadas"] += 1
            continue

            # existing case
        idx = next((i for i, v in enumerate((existing.get("versiones") or [])) if int(v.get("version")) == int(versionNum)), -1)

        if idx >= 0 and not overwrite_version:
            res["versionesRechazadas"] += 1
            warnings.append(f"Versión ya existe y no se sobreescribe: sku_PT='{skuPT}' v={versionNum}")
            continue

        if dry_run:
            if idx >= 0: res["recetasActualizadas"] += 1
            else: res["versionesAgregadas"] += 1
            if marcar_vigente: res["vigentesSeteadas"] += 1
        else:
            if idx >= 0:
                # replace versión existente
                set_obj = {f"versiones.{idx}": version_doc, "audit.updatedAt": now}
                await RECIPES.update_one({"_id": existing["_id"]}, {"$set": set_obj})
                res["recetasActualizadas"] += 1
            else:
                upd = {"$push": {"versiones": version_doc}, "$set": {"audit.updatedAt": now}}
                if marcar_vigente:
                    upd["$set"]["vigenteVersion"] = int(versionNum)
                    res["vigentesSeteadas"] += 1
                await RECIPES.update_one({"_id": existing["_id"]}, upd)
                res["versionesAgregadas"] += 1

    return res
