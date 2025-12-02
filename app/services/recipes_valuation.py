# app/services/recipes_valuation.py
from __future__ import annotations
from typing import Dict, Any, List, Literal
from datetime import datetime, timezone
from bson import ObjectId

from app.db.repositories import recipes_repo

CostMethod = Literal["pneto", "piva", "last"]

def _num_or_zero(v) -> float:
    try:
        n = float(v)
        return n if n == n and n != float("inf") and n != float("-inf") else 0.0
    except Exception:
        return 0.0

def _get_unit_cost(product: Dict[str, Any], method: CostMethod) -> float:
    if method == "pneto":
        val = product.get("pneto")
        if val is None:
            val = product.get("last")
        return _num_or_zero(val)

    if method == "piva":
        val = product.get("piva")
        if val is None:
            pn = _num_or_zero(product.get("pneto"))
            if pn > 0:
                return round(pn * 1.19, 6)
            val = product.get("last")
        return _num_or_zero(val)

    # method == "last"
    val = product.get("last")
    if val is None:
        val = product.get("pneto")
    return _num_or_zero(val)

async def preview_valuation(
    db,
    *,
    skuPT: str,
    version: int,
    cost_method: CostMethod = "pneto",
    currency: str = "CLP",
    debug: bool = False,  # 👈 NUEVO
) -> Dict[str, Any]:
    # 1) Resolver PT y receta
    pt = await recipes_repo.get_pt_by_sku(skuPT, db)
    if not pt:
        raise ValueError("PT no encontrado")
    rec = await recipes_repo.find_by_pt_id(pt["_id"], db=db)
    if not rec:
        raise ValueError("Receta no encontrada")

    ver = next((v for v in rec.get("versiones", []) if int(v.get("version")) == int(version)), None)
    if not ver:
        raise ValueError(f"Versión {version} no encontrada")

    componentes = ver.get("componentes", []) or []

    warnings: List[str] = []
    breakdown: List[Dict[str, Any]] = []
    debug_rows: List[Dict[str, Any]] = []  # 👈 NUEVO

    prod_col = await recipes_repo.products_coll(db)

    # 2) Valorización de materiales
    for comp in componentes:
        pid_raw = comp.get("productId")
        # Siempre enviaremos strings (nunca None) para cumplir con Pydantic
        safe_pid_str = ""
        try:
            oid = pid_raw if isinstance(pid_raw, ObjectId) else ObjectId(str(pid_raw))
            safe_pid_str = str(oid)
        except Exception:
            warnings.append(f"[WARN] component productId inválido → {pid_raw!r}")
            breakdown.append({
                "sku": "",                 # string vacío en vez de None
                "productId": str(pid_raw) if pid_raw is not None else "",
                "descripcion": "",
                "unidad": "",
                "unit_cost": 0.0,
                "qty_eff": 0.0,
                "subtotal": 0.0,
            })
            if debug:
                debug_rows.append({
                    "component": comp,
                    "error": "productId inválido",
                    "pid_raw": pid_raw,
                })
            continue

        prod = await prod_col.find_one(
            {"_id": oid},
            {
                "sku": 1,
                # nombres posibles
                "nombre": 1,
                # unidades posibles
                "unidad": 1,
                # costos
                "pneto": 1, "piva": 1, "last": 1,
            },
        )
        if not prod:
            warnings.append(f"[WARN] productId no encontrado → _id={safe_pid_str}")
            breakdown.append({
                "sku": "",
                "productId": safe_pid_str,
                "descripcion": "",
                "unidad": "",
                "unit_cost": 0.0,
                "qty_eff": 0.0,
                "subtotal": 0.0,
            })
            if debug:
                debug_rows.append({
                    "_id": safe_pid_str,
                    "found": False,
                    "component": comp,
                })
            continue

        sku = prod.get("sku")
        if not isinstance(sku, str) or not sku.strip():
            warnings.append(f"[WARN] producto _id={safe_pid_str} sin SKU")
            sku = ""  # string vacío

        # fallbacks de nombre/unidad (si faltan, string vacío)
        nombre = (
            prod.get("nombre")
            or ""
        )
        if not isinstance(nombre, str):
            nombre = ""
            warnings.append(f"[WARN] producto _id={safe_pid_str} sku={sku} sin nombre")

        unidad = (
            prod.get("unidad")
            or ""
        )
        if not isinstance(unidad, str):
            unidad = ""
            warnings.append(f"[WARN] producto _id={safe_pid_str} sku={sku} sin unidad")

        qty_base = _num_or_zero(comp.get("cantidadPorBase"))
        merma = _num_or_zero(comp.get("merma_pct"))
        qty_eff = round(qty_base * (1.0 + merma / 100.0), 6)

        unit_cost = _get_unit_cost(prod, cost_method)
        if unit_cost == 0.0:
            warnings.append(f"[WARN] sku={sku or safe_pid_str} sin costo '{cost_method}', usando 0")

        subtotal = round(qty_eff * unit_cost, 6)

        breakdown.append({
            "sku": sku,                  # SIEMPRE string
            "productId": safe_pid_str,   # SIEMPRE string
            "descripcion": nombre,       # SIEMPRE string
            "unidad": unidad,            # SIEMPRE string
            "unit_cost": unit_cost,
            "qty_eff": qty_eff,
            "subtotal": subtotal,
        })

        if debug:
            debug_rows.append({
                "_id": safe_pid_str,
                "sku": sku,
                "nombre": nombre,
                "unidad": unidad,
                "costs": {
                    "pneto": prod.get("pneto"),
                    "piva": prod.get("piva"),
                    "last": prod.get("last"),
                    "used_method": cost_method,
                    "unit_cost_used": unit_cost,
                },
                "calc": {
                    "cantidadPorBase": comp.get("cantidadPorBase"),
                    "merma_pct": comp.get("merma_pct"),
                    "qty_eff": qty_eff,
                    "subtotal": subtotal,
                },
            })

    # 3) Costo de proceso (si aplica)
    process_cost = 0.0
    if ver.get("procesoEspecial_costo") is not None:
        process_cost = _num_or_zero(ver["procesoEspecial_costo"])

    total_materiales = round(sum(x["subtotal"] for x in breakdown), 6)
    total = round(total_materiales + process_cost, 6)

    resp: Dict[str, Any] = {
        "skuPT": skuPT,
        "version": int(version),
        "currency": currency,
        "breakdown": breakdown,
        "process_cost": process_cost,
        "total": total,
        "valued_at": datetime.now(timezone.utc).isoformat(),
        "warnings": warnings,
    }
    if debug:
        resp["_debug"] = debug_rows
    return resp

async def value_version(
    db,
    skuPT: str,
    version: int,
    cost_method: Literal["pneto", "piva", "last"] = "pneto",
    currency: str = "CLP",
    persist: bool = False,
) -> Dict[str, Any]:
    return await preview_valuation(
        db,
        skuPT=skuPT,
        version=version,
        cost_method=cost_method,
        currency=currency,
    )
