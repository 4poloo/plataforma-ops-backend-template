from __future__ import annotations

from datetime import date
from typing import Literal, Optional

from fastapi import APIRouter, Depends, Query, status

from app.db.mongo import get_db
from app.models.logs import LogCreateIn, LogOut, LogsListFilters, LogsListOut
from app.services import logs_service

router = APIRouter(prefix="/logs", tags=["logs"])


@router.post("", response_model=LogOut, status_code=status.HTTP_201_CREATED)
async def create_log(body: LogCreateIn, db=Depends(get_db)):
    """
    Registra un nuevo log proveniente del front.

    - Calcula la severidad automáticamente (WARN para disable/delete, INFO para el resto).
    - Construye el campo `accion` como actor.entidad.evento.
    - Normaliza el alias para búsquedas case-insensitive.
    """
    return await logs_service.create_log(db, body)


@router.get("", response_model=LogsListOut)
async def list_logs(
    q: Optional[str] = Query(
        None,
        description="Filtro LIKE (regex) por alias del usuario.",
    ),
    severity: Optional[Literal["INFO", "WARN"]] = Query(
        None,
        description="Filtrar por severidad.",
    ),
    date_filter: Optional[date] = Query(
        None,
        alias="date",
        description="Filtra por fecha (sin hora). Formato ISO YYYY-MM-DD.",
    ),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db=Depends(get_db),
):
    filters = LogsListFilters(q=q, severity=severity, date=date_filter)
    items, total = await logs_service.list_logs(db, filters, skip=skip, limit=limit)
    return {"items": items, "total": total, "skip": skip, "limit": limit}
