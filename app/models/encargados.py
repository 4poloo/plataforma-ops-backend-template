from __future__ import annotations

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional


def _validate_non_empty(value: str, field_name: str) -> str:
    if value is None:
        raise ValueError(f"{field_name} es requerido")
    text = value.strip()
    if not text:
        raise ValueError(f"{field_name} no puede estar vacío")
    return text


class EncargadoBase(BaseModel):
    nombre: str = Field(..., min_length=1, max_length=200)
    linea: str = Field(..., min_length=1, max_length=200)
    predeterminado: bool = Field(False, description="Indica si el encargado es el predeterminado para la línea")

    @field_validator("nombre", mode="before")
    def _nombre_strip(cls, v: str) -> str:
        return _validate_non_empty(v, "nombre")

    @field_validator("linea", mode="before")
    def _linea_strip(cls, v: str) -> str:
        return _validate_non_empty(v, "linea")


class EncargadoCreate(EncargadoBase):
    """Payload para crear un encargado."""


class EncargadoUpdate(BaseModel):
    nombre: Optional[str] = Field(None, min_length=1, max_length=200)
    linea: Optional[str] = Field(None, min_length=1, max_length=200)
    predeterminado: Optional[bool] = None

    @field_validator("nombre", mode="before")
    def _nombre_strip(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        return _validate_non_empty(v, "nombre")

    @field_validator("linea", mode="before")
    def _linea_strip(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        return _validate_non_empty(v, "linea")

    @model_validator(mode="after")
    def _require_any(cls, values):
        if values.nombre is None and values.linea is None and values.predeterminado is None:
            raise ValueError("Debe enviar al menos un campo para actualizar")
        return values


class EncargadoOut(BaseModel):
    id: str = Field(..., alias="_id")
    nombre: str
    linea: str
    predeterminado: bool
