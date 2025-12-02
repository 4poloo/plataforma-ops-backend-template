from __future__ import annotations

from datetime import date, datetime
from typing import Any, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, FieldValidationInfo, model_validator


class WorkOrderContentIn(BaseModel):
    SKU: str = Field(..., min_length=1)
    Cantidad: float = Field(..., description="Cantidad solicitada", gt=0)
    Encargado: str = Field(..., min_length=1)
    linea: str = Field(..., min_length=1)
    fecha: date
    fecha_ini: date
    fecha_fin: date
    descripcion: Optional[str] = Field(
        None,
        min_length=1,
        description="Nombre o descripción del producto asociado a la OT.",
    )


class WorkOrderCreateIn(BaseModel):
    OT: int = Field(..., ge=1)
    contenido: WorkOrderContentIn
    estado: Literal["CREADA", "EN PROCESO", "CERRADA"] = Field(
        "CREADA", description="Estado actual de la OT."
    )
    merma: float = Field(0, ge=0, description="Cantidad merma registrada para la OT.")
    cantidad_fin: float = Field(
        0,
        ge=0,
        description="Cantidad final producida para la OT.",
    )


class AuditOut(BaseModel):
    createdAt: datetime
    updatedAt: datetime


class WorkOrderContentOut(BaseModel):
    SKU: str
    Cantidad: float
    Encargado: str
    linea: str
    fecha: date
    fecha_ini: date
    fecha_fin: date
    descripcion: Optional[str] = None


class WorkOrderOut(BaseModel):
    id: str = Field(..., alias="_id")
    OT: int
    contenido: WorkOrderContentOut
    estado: Literal["CREADA", "EN PROCESO", "CERRADA"]
    merma: float = Field(..., ge=0)
    cantidad_fin: float = Field(..., ge=0)
    audit: AuditOut


class RecipePrintRequest(BaseModel):
    skuPT: str = Field(..., min_length=1)
    cantidad: Optional[float] = Field(None, ge=0)
    numeroOT: Optional[str] = None
    encargado: Optional[str] = None
    fecha_ini: Optional[date] = None

    @field_validator("skuPT", "numeroOT", "encargado", mode="before")
    @classmethod
    def _strip_strings(cls, v: Optional[str], info: FieldValidationInfo):
        if v is None:
            return v
        text = str(v).strip()
        if info.field_name == "skuPT" and not text:
            raise ValueError("skuPT es requerido")
        return text


class WorkOrderIntegrationItem(BaseModel):
    FecIniOrden: str = Field(..., min_length=1, description="Fecha de inicio de la orden (formato dd/mm/YYYY)")
    GlosaOrden: str = Field(..., min_length=1)
    Orden: str = Field(..., description="Identificador numérico de la OT en WMS")
    CodigoProducto: str = Field(..., min_length=1)
    DescripcionProducto: str = Field(..., min_length=1)
    CantidadAFabricar: float = Field(..., gt=0)
    CodigoMaterial: str = Field(..., min_length=1)
    DescripcionMaterial: str = Field(..., min_length=1)
    CantidadMaterial: float = Field(..., gt=0)

    @field_validator("Orden", mode="before")
    @classmethod
    def _orden_to_str(cls, v: Any) -> str:
        if v is None:
            raise ValueError("Orden es requerido")
        text = str(v).strip()
        if not text:
            raise ValueError("Orden es requerido")
        return text

    @field_validator("CantidadAFabricar", "CantidadMaterial", mode="before")
    @classmethod
    def _parse_float(cls, v: Any, info: FieldValidationInfo) -> float:
        if v is None:
            raise ValueError(f"{info.field_name} es requerido")
        try:
            return float(v)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{info.field_name} inválido") from exc


class WorkOrderIntegrationRequest(BaseModel):
    source: str = Field("portal", min_length=1)
    payload: Optional[List[WorkOrderIntegrationItem]] = None
    OT: Optional[int] = None
    contenido: Optional[WorkOrderContentIn] = None

    @model_validator(mode="after")
    def _require_payload_or_work_order(self):
        if self.payload is not None and len(self.payload) == 0:
            raise ValueError("El 'payload' no puede estar vacío.")
        if not self.payload and (self.OT is None or self.contenido is None):
            raise ValueError("Debe enviar 'payload' o los datos de la OT (OT + contenido).")
        return self


class WorkOrderIntegrationResponse(BaseModel):
    status_code: int
    body: Optional[Any] = None


class WorkOrderStatusOut(BaseModel):
    code: str
    state: str
    state_raw: str
    site: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class WorkOrderListFilters(BaseModel):
    estado: Optional[Literal["CREADA", "EN PROCESO", "CERRADA"]] = None


class WorkOrderEstadoUpdate(BaseModel):
    estado: Literal["CREADA", "EN PROCESO", "CERRADA"]


class NextWorkOrderOut(BaseModel):
    next: int = Field(..., ge=1)


class LastWorkOrderOut(BaseModel):
    OT: int = Field(..., ge=1)


class WorkOrderImportError(BaseModel):
    row: int
    ot: Optional[int] = None
    error: str


class WorkOrderBulkImportOut(BaseModel):
    created: list[WorkOrderOut]
    errors: list[WorkOrderImportError]
    wms_response: Optional[WorkOrderIntegrationResponse] = None
    wms_error: Optional[str] = None
    wms_error_detail: Optional[Any] = None
