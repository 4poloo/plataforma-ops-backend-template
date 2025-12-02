from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from app.db.mongo import get_db
from app.db.repositories import counters_repo
from app.models.counters import CounterOut, CounterUpdateIn, CounterIncrementIn, CounterRollbackIn

router = APIRouter(prefix="/counters", tags=["counters"])


def _to_out(doc: dict) -> CounterOut:
    data = {**doc}
    data["id"] = str(data.pop("_id"))
    data["seq"] = int(data.get("seq", 0) or 0)
    return CounterOut(**data)


@router.get("/{counter_id}", response_model=CounterOut)
async def get_counter(counter_id: str, db=Depends(get_db)):
    doc = await counters_repo.find_by_id(counter_id, db=db)
    if not doc:
        raise HTTPException(status_code=404, detail="Contador no encontrado")
    return _to_out(doc)


@router.patch("/{counter_id}", response_model=CounterOut)
async def update_counter(counter_id: str, body: CounterUpdateIn, db=Depends(get_db)):
    updated = await counters_repo.update_seq(counter_id, body.seq, db=db)
    if not updated:
        raise HTTPException(status_code=404, detail="Contador no encontrado")
    return _to_out(updated)


@router.post("/{counter_id}/next", response_model=CounterOut)
async def increment_counter(counter_id: str, body: CounterIncrementIn | None = None, db=Depends(get_db)):
    step = body.step if body else 1
    updated = await counters_repo.increment_seq(counter_id, step=step, db=db)
    if not updated:
        raise HTTPException(status_code=404, detail="Contador no encontrado")
    return _to_out(updated)


@router.post("/{counter_id}/rollback", response_model=CounterOut)
async def rollback_counter(counter_id: str, body: CounterRollbackIn | None = None, db=Depends(get_db)):
    step = body.step if body else 1
    updated = await counters_repo.decrement_seq(counter_id, step=step, db=db)
    if updated:
        return _to_out(updated)

    existing = await counters_repo.find_by_id(counter_id, db=db)
    if not existing:
        raise HTTPException(status_code=404, detail="Contador no encontrado")
    raise HTTPException(
        status_code=409,
        detail="No se pudo revertir: el valor actual es menor al decremento solicitado",
    )
