from __future__ import annotations

from datetime import date, datetime, time, timezone, timedelta
from typing import Any, Dict, List

from bson import ObjectId

from app.db.repositories import gestion_ot_prod_repo, work_orders_repo
from app.models.gestion_produccion import (
    GestionOTProdCreateIn,
    GestionOTProdFilters,
    GestionOTProdUpdateIn,
)


def _oid_str(oid: ObjectId | None) -> str | None:
    return str(oid) if oid is not None else None


def _normalize_date(value: Any, field_name: str) -> date:
    if value is None:
        raise ValueError(f"{field_name} es requerido")

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(f"{field_name} es requerido")
        for parser in (
            lambda v: datetime.fromisoformat(v.replace("Z", "+00:00")).date(),
            lambda v: datetime.strptime(v, "%Y-%m-%d").date(),
            lambda v: datetime.strptime(v, "%d-%m-%Y").date(),
            lambda v: datetime.strptime(v, "%d/%m/%Y").date(),
            lambda v: datetime.strptime(v, "%Y/%m/%d").date(),
        ):
            try:
                return parser(text)
            except ValueError:
                continue
        raise ValueError(f"{field_name} con formato inválido: '{value}'")
    raise ValueError(f"{field_name} con formato inválido: '{value}'")


def _as_datetime(value: date) -> datetime:
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def _normalize_time(value: Any, field_name: str) -> time:
    if value is None:
        raise ValueError(f"{field_name} es requerido")
    if isinstance(value, time):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(f"{field_name} es requerido")
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return datetime.strptime(text, fmt).time()
            except ValueError:
                continue
        raise ValueError(f"{field_name} con formato inválido: '{value}'")
    raise ValueError(f"{field_name} con formato inválido: '{value}'")


def _map_entry(doc: Dict[str, Any]) -> Dict[str, Any]:
    contenido = doc.get("contenido", {})

    def _to_date(raw: Any) -> date | None:
        if raw is None:
            return None
        if isinstance(raw, datetime):
            return raw.date()
        if isinstance(raw, date):
            return raw
        try:
            return _normalize_date(raw, "fecha")
        except ValueError:
            return None

    def _to_time(raw: Any) -> time | None:
        if raw is None:
            return None
        try:
            return _normalize_time(raw, "hora_entrega")
        except ValueError:
            return None

    return {
        "_id": _oid_str(doc.get("_id")),
        "OT": doc.get("OT"),
        "contenido": {
            "SKU": contenido.get("SKU"),
            "Encargado": contenido.get("Encargado"),
            "linea": contenido.get("linea"),
            "fecha": _to_date(contenido.get("fecha")),
            "fecha_ini": _to_date(contenido.get("fecha_ini")),
            "fecha_fin": _to_date(contenido.get("fecha_fin")),
            "hora_entrega": _to_time(contenido.get("hora_entrega")),
            "descripcion": contenido.get("descripcion"),
            "cantidad_hora_extra": float(contenido.get("cantidad_hora_extra", 0) or 0),
            "cantidad_hora_normal": float(contenido.get("cantidad_hora_normal", 0) or 0),
        },
        "estado": doc.get("estado", "CREADA"),
        "merma": float(doc.get("merma", 0) or 0),
        "cantidad_fin": float(doc.get("cantidad_fin", 0) or 0),
        "audit": doc.get("audit", {}),
    }


async def create_entry(db, payload: GestionOTProdCreateIn) -> Dict[str, Any]:
    existing = await gestion_ot_prod_repo.find_by_ot(int(payload.OT), db=db)
    if existing:
        raise ValueError(f"La OT {payload.OT} ya existe en Gestión de Producción.")

    contenido = payload.contenido
    fecha = _normalize_date(contenido.fecha, "fecha")
    fecha_ini = _normalize_date(contenido.fecha_ini, "fecha_ini")
    fecha_fin = _normalize_date(contenido.fecha_fin, "fecha_fin")
    hora_entrega = _normalize_time(contenido.hora_entrega, "hora_entrega")

    if fecha_ini > fecha_fin:
        raise ValueError("fecha_ini no puede ser mayor a fecha_fin")

    now = datetime.now(timezone.utc)
    descripcion = (
        contenido.descripcion.strip()
        if isinstance(contenido.descripcion, str) and contenido.descripcion.strip()
        else None
    )

    doc: Dict[str, Any] = {
        "OT": int(payload.OT),
        "contenido": {
            "SKU": contenido.SKU,
            "Encargado": contenido.Encargado,
            "linea": contenido.linea,
            "fecha": _as_datetime(fecha),
            "fecha_ini": _as_datetime(fecha_ini),
            "fecha_fin": _as_datetime(fecha_fin),
            "hora_entrega": hora_entrega.strftime("%H:%M"),
            "descripcion": descripcion,
            "cantidad_hora_extra": float(contenido.cantidad_hora_extra),
            "cantidad_hora_normal": float(contenido.cantidad_hora_normal),
        },
        "estado": payload.estado,
        "merma": float(payload.merma),
        "cantidad_fin": float(payload.cantidad_fin),
        "audit": {"createdAt": now, "updatedAt": now},
    }

    saved = await gestion_ot_prod_repo.insert_entry(doc, db=db)
    return _map_entry(saved)


async def list_entries(
    db,
    *,
    limit: int = 50,
    skip: int = 0,
    filters: GestionOTProdFilters | None = None,
) -> List[Dict[str, Any]]:
    filtro_db: Dict[str, Any] = {}
    expr_conditions: List[Dict[str, Any]] = []

    if filters:
        if filters.ot is not None:
            filtro_db["OT"] = int(filters.ot)
        if filters.fecha is not None:
            start = datetime.combine(filters.fecha, time.min, tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            filtro_db["contenido.fecha"] = {"$gte": start, "$lt": end}
        if filters.hora is not None:
            expr_conditions.append(
                {"$eq": [{"$hour": "$audit.createdAt"}, int(filters.hora)]}
            )

    if expr_conditions:
        expr = expr_conditions[0] if len(expr_conditions) == 1 else {"$and": expr_conditions}
        existing_expr = filtro_db.get("$expr")
        if existing_expr:
            expr = {"$and": [existing_expr, expr]}
        filtro_db["$expr"] = expr

    docs = await gestion_ot_prod_repo.list_entries(
        db=db,
        limit=limit,
        skip=skip,
        filtro=filtro_db or None,
        sort=[("audit.createdAt", -1)],
    )
    return [_map_entry(doc) for doc in docs]


async def update_entry(db, ot: int, payload: GestionOTProdUpdateIn) -> Dict[str, Any]:
    try:
        ot_int = int(ot)
    except (TypeError, ValueError) as exc:
        raise ValueError("El número de OT debe ser un entero") from exc

    existing = await gestion_ot_prod_repo.find_by_ot(ot_int, db=db)
    if not existing:
        raise ValueError("OT no encontrada en Gestión de Producción.")

    contenido = existing.get("contenido", {})
    set_fields: Dict[str, Any] = {}

    if payload.estado is not None:
        set_fields["estado"] = payload.estado

    fecha_ini_actual = None
    if contenido.get("fecha_ini") is not None:
        try:
            fecha_ini_actual = _normalize_date(contenido.get("fecha_ini"), "fecha_ini")
        except ValueError:
            fecha_ini_actual = None

    fecha_fin_actual = None
    if contenido.get("fecha_fin") is not None:
        try:
            fecha_fin_actual = _normalize_date(contenido.get("fecha_fin"), "fecha_fin")
        except ValueError:
            fecha_fin_actual = None

    fecha_ini_final = fecha_ini_actual
    if payload.fecha_ini is not None:
        fecha_ini_final = _normalize_date(payload.fecha_ini, "fecha_ini")
        set_fields["contenido.fecha_ini"] = _as_datetime(fecha_ini_final)

    fecha_fin_final = fecha_fin_actual
    if payload.fecha_fin is not None:
        fecha_fin_final = _normalize_date(payload.fecha_fin, "fecha_fin")
        set_fields["contenido.fecha_fin"] = _as_datetime(fecha_fin_final)

    if (
        fecha_ini_final is not None
        and fecha_fin_final is not None
        and fecha_ini_final > fecha_fin_final
    ):
        raise ValueError("fecha_ini no puede ser mayor a fecha_fin")

    if payload.cantidad_hora_extra is not None:
        set_fields["contenido.cantidad_hora_extra"] = float(payload.cantidad_hora_extra)

    if payload.cantidad_hora_normal is not None:
        set_fields["contenido.cantidad_hora_normal"] = float(payload.cantidad_hora_normal)

    if payload.hora_entrega is not None:
        hora_entrega = _normalize_time(payload.hora_entrega, "hora_entrega")
        set_fields["contenido.hora_entrega"] = hora_entrega.strftime("%H:%M")

    if payload.descripcion is not None:
        text = payload.descripcion.strip()
        if not text:
            raise ValueError("descripcion no puede estar vacía.")
        set_fields["contenido.descripcion"] = text

    if not set_fields:
        raise ValueError("Debe indicar al menos un campo para actualizar.")

    updated = await gestion_ot_prod_repo.update_fields_by_ot(
        ot_int,
        set_fields,
        db=db,
    )
    if not updated:
        raise ValueError("OT no encontrada en Gestión de Producción.")
    return _map_entry(updated)


async def close_previous_day_entries(db) -> Dict[str, int]:
    """
    Cierra en bloque las OT cuya fecha sea anterior al día actual (UTC).
    Así también se cubre backlog que quedó abierto más de un día.
    Actualiza tanto gestion_OT_prod como work_orders (solo el campo estado).
    """
    now = datetime.now(timezone.utc)
    start_today = datetime.combine(now.date(), time.min, tzinfo=timezone.utc)
    closed_gestion = await gestion_ot_prod_repo.close_until_fecha(start_today, db=db)
    closed_work_orders = await work_orders_repo.close_until_fecha(start_today, db=db)
    return {"gestion_ot_prod": closed_gestion, "work_orders": closed_work_orders}
