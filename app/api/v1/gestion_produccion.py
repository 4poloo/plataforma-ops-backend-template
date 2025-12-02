from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from app.db.mongo import get_db
from app.models.gestion_produccion import (
    GestionOTProdCreateIn,
    GestionOTProdFilters,
    GestionOTProdOut,
    GestionOTProdUpdateIn,
)
from app.services import gestion_ot_prod

router = APIRouter(prefix="/gestion-produccion", tags=["gestion-produccion"])


@router.get("", response_model=list[GestionOTProdOut])
async def list_gestion_ot_entries(
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    ot: int | None = Query(None, ge=1, description="Número de OT exacto."),
    fecha: date | None = Query(None, description="Filtra por día usando contenido.fecha."),
    hora: int | None = Query(
        None,
        ge=0,
        le=23,
        description="Hora del día (0-23) basada en audit.createdAt (UTC).",
    ),
    db=Depends(get_db),
):
    has_filters = any(value is not None for value in (ot, fecha, hora))
    filters = GestionOTProdFilters(ot=ot, fecha=fecha, hora=hora) if has_filters else None
    return await gestion_ot_prod.list_entries(
        db,
        limit=limit,
        skip=skip,
        filters=filters,
    )


@router.post("", response_model=GestionOTProdOut)
async def create_gestion_ot_entry(body: GestionOTProdCreateIn, db=Depends(get_db)):
    try:
        return await gestion_ot_prod.create_entry(db, body)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@router.patch("/{ot}", response_model=GestionOTProdOut)
@router.patch("/{ot}/estado", response_model=GestionOTProdOut)
async def update_gestion_ot_entry(
    ot: int,
    body: GestionOTProdUpdateIn,
    db=Depends(get_db),
):
    try:
        return await gestion_ot_prod.update_entry(db, ot, body)
    except ValueError as exc:
        message = str(exc)
        status = 404 if "no encontrada" in message.lower() else 422
        raise HTTPException(status_code=status, detail=message)
