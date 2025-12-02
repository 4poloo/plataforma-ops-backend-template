from __future__ import annotations

from datetime import date, time
from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator

from app.models.work_orders import AuditOut


class GestionOTProdContentIn(BaseModel):
    SKU: str = Field(..., min_length=1)
    Encargado: str = Field(..., min_length=1)
    linea: str = Field(..., min_length=1)
    fecha: date
    fecha_ini: date
    fecha_fin: date
    hora_entrega: time = Field(..., description="Hora compromiso de entrega (HH:MM 24h).")
    descripcion: Optional[str] = Field(
        None,
        min_length=1,
        description="Observaciones o detalle adicional de la OT.",
    )
    cantidad_hora_extra: float = Field(
        ...,
        ge=0,
        description="Horas cargadas como extra para la OT.",
    )
    cantidad_hora_normal: float = Field(
        ...,
        ge=0,
        description="Horas cargadas como normales para la OT.",
    )


class GestionOTProdCreateIn(BaseModel):
    OT: int = Field(..., ge=1)
    contenido: GestionOTProdContentIn
    estado: Literal["CREADA", "EN PROCESO", "CERRADA"] = Field(
        "CREADA", description="Estado actual de la OT.",
    )
    merma: float = Field(0, ge=0, description="Cantidad merma registrada para la OT.")
    cantidad_fin: float = Field(
        0,
        ge=0,
        description="Cantidad final producida para la OT.",
    )


class GestionOTProdContentOut(BaseModel):
    SKU: str
    Encargado: str
    linea: str
    fecha: date
    fecha_ini: date
    fecha_fin: date
    hora_entrega: time
    descripcion: Optional[str] = None
    cantidad_hora_extra: float
    cantidad_hora_normal: float


class GestionOTProdOut(BaseModel):
    id: str = Field(..., alias="_id")
    OT: int
    contenido: GestionOTProdContentOut
    estado: Literal["CREADA", "EN PROCESO", "CERRADA"]
    merma: float = Field(..., ge=0)
    cantidad_fin: float = Field(..., ge=0)
    audit: AuditOut


class GestionOTProdFilters(BaseModel):
    ot: Optional[int] = Field(None, ge=1)
    fecha: Optional[date] = None
    hora: Optional[int] = Field(
        None,
        ge=0,
        le=23,
        description="Hora del día (0-23) basada en audit.createdAt (UTC).",
    )


class GestionOTProdUpdateIn(BaseModel):
    estado: Optional[Literal["CREADA", "EN PROCESO", "CERRADA"]] = None
    fecha_ini: Optional[date] = None
    fecha_fin: Optional[date] = None
    hora_entrega: Optional[time] = None
    descripcion: Optional[str] = Field(None, min_length=1)
    cantidad_hora_extra: Optional[float] = Field(None, ge=0)
    cantidad_hora_normal: Optional[float] = Field(None, ge=0)

    @model_validator(mode="after")
    def _require_fields(cls, values):
        if not any(
            value is not None
            for value in (
                values.estado,
                values.fecha_ini,
                values.fecha_fin,
                values.cantidad_hora_extra,
                values.cantidad_hora_normal,
                values.hora_entrega,
                values.descripcion,
            )
        ):
            raise ValueError("Debe indicar al menos un campo para actualizar.")
        return values
