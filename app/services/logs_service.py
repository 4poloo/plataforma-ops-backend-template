from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Tuple

from bson import ObjectId

from app.db.repositories import logs_repo
from app.models.logs import LogCreateIn, LogsListFilters


def _oid_str(oid: ObjectId | None) -> str | None:
    return str(oid) if oid is not None else None


def _ensure_timezone(dt: datetime | None) -> datetime:
    if dt is None:
        return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _determine_severity(explicit: str | None, event: str) -> str:
    if explicit in {"INFO", "WARN"}:
        return explicit
    if event in {"disable", "delete"}:
        return "WARN"
    return "INFO"


def _map_out(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": _oid_str(doc.get("_id")),
        "loggedAt": doc.get("loggedAt"),
        "severity": doc.get("severity"),
        "accion": doc.get("accion"),
        "usuario": doc.get("usuario"),
        "payload": doc.get("payload") or {},
        "actor": doc.get("actor"),
        "entity": doc.get("entity"),
        "event": doc.get("event"),
    }


async def create_log(db, payload: LogCreateIn) -> Dict[str, Any]:
    alias_clean = payload.userAlias.strip()
    logged_at = _ensure_timezone(payload.loggedAt)
    severity = _determine_severity(payload.severity, payload.event)
    accion = f"{payload.actor}.{payload.entity}.{payload.event}"

    doc = {
        "loggedAt": logged_at,
        "severity": severity,
        "accion": accion,
        "usuario": alias_clean,
        "userAlias_ci": alias_clean.lower(),
        "payload": payload.payload or {},
        "actor": payload.actor,
        "entity": payload.entity,
        "event": payload.event,
    }

    created = await logs_repo.insert_log(doc, db=db)
    return _map_out(created)


def _build_filters(filters: LogsListFilters) -> Dict[str, Any]:
    filtro: Dict[str, Any] = {}
    if filters.q:
        filtro["actor"] = {"$regex": re.escape(filters.q), "$options": "i"}
    if filters.severity:
        filtro["severity"] = filters.severity
    if filters.date:
        start = datetime.combine(filters.date, datetime.min.time()).replace(
            tzinfo=timezone.utc
        )
        end = start + timedelta(days=1)
        filtro["loggedAt"] = {"$gte": start, "$lt": end}
    return filtro


async def list_logs(
    db,
    filters: LogsListFilters,
    skip: int,
    limit: int,
) -> Tuple[list[Dict[str, Any]], int]:
    filtro = _build_filters(filters)
    docs = await logs_repo.list_logs(
        filtro=filtro, skip=skip, limit=limit, sort=[("loggedAt", -1)], db=db
    )
    total = await logs_repo.count_logs(filtro=filtro, db=db)
    return [_map_out(doc) for doc in docs], total
