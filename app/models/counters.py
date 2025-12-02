from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class CounterOut(BaseModel):
    id: str
    seq: int

    model_config = ConfigDict(extra="allow")


class CounterUpdateIn(BaseModel):
    seq: int = Field(..., ge=0, description="Nuevo valor para el campo seq.")


class CounterIncrementIn(BaseModel):
    step: int = Field(1, ge=1, description="Cantidad a incrementar.")


class CounterRollbackIn(BaseModel):
    step: int = Field(1, ge=1, description="Cantidad a revertir del contador.")
