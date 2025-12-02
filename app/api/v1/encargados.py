from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from app.db.mongo import get_db
from app.models.encargados import EncargadoCreate, EncargadoOut, EncargadoUpdate
from app.services import encargados_service

router = APIRouter(prefix="/encargados", tags=["encargados"])


@router.get("", response_model=list[EncargadoOut])
async def list_encargados(
    linea: str | None = Query(None),
    nombre: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    skip: int = Query(0, ge=0),
    db=Depends(get_db),
):
    return await encargados_service.list_encargados(
        db,
        linea=linea,
        nombre=nombre,
        limit=limit,
        skip=skip,
    )


@router.post("", response_model=EncargadoOut, status_code=201)
async def create_encargado(body: EncargadoCreate, db=Depends(get_db)):
    try:
        return await encargados_service.create_encargado(db, body)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@router.put("/{encargado_id}", response_model=EncargadoOut)
async def update_encargado(encargado_id: str, body: EncargadoUpdate, db=Depends(get_db)):
    try:
        return await encargados_service.update_encargado(db, encargado_id, body)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

