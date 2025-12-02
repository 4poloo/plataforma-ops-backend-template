from __future__ import annotations

from datetime import datetime, date as date_type
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class LogPayload(BaseModel):
    """Representa el payload libre que envía el front."""

    model_config = ConfigDict(extra="allow")


class LogCreateIn(BaseModel):
    """Datos necesarios para crear un log desde el front."""

    actor: Literal["admin", "user", "sistema"] = Field(
        ...,
        description="Tipo de usuario que ejecutó la acción (admin, user o sistema).",
    )
    entity: Literal["recipe", "user", "encargado", "work_order", "product"] = Field(
        ...,
        description="Recurso afectado por la acción.",
    )
    event: Literal["create", "update", "modify", "disable", "delete", "enable"] = Field(
        ...,
        description="Acción realizada sobre el recurso.",
    )
    userAlias: str = Field(
        ...,
        min_length=1,
        description="Alias del usuario que ejecutó la acción.",
    )
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Payload original enviado desde el front.",
    )
    severity: Optional[Literal["INFO", "WARN"]] = Field(
        default=None,
        description="Severidad manual (INFO o WARN). Si no se envía se calcula automáticamente.",
    )
    loggedAt: Optional[datetime] = Field(
        default=None,
        description="Permite registrar un timestamp custom. Por defecto usa now() en UTC.",
    )


class LogOut(BaseModel):
    """Respuesta serializada de un log almacenado."""

    id: str
    loggedAt: datetime
    severity: Literal["INFO", "WARN"]
    accion: str
    usuario: str
    payload: Dict[str, Any]

    model_config = ConfigDict(extra="allow")


class LogsListOut(BaseModel):
    """Respuesta paginada para listados."""

    items: list[LogOut]
    total: int
    skip: int
    limit: int


class LogsListFilters(BaseModel):
    """Modelo auxiliar para parsear filtros vía query params."""

    q: Optional[str] = Field(
        default=None,
        description="Filtro LIKE sobre el alias del usuario (case-insensitive).",
    )
    severity: Optional[Literal["INFO", "WARN"]] = Field(
        default=None, description="Filtra por severidad."
    )
    date: Optional[date_type] = Field(
        default=None,
        description="Filtra por fecha (ignora hora).",
    )
