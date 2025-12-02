# app/schemas/recipes.py
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Literal
from datetime import date, datetime

EstadoVersion = Literal["borrador", "vigente", "obsoleta"]

# -------- Entradas (request) --------
class RecetaComponenteIn(BaseModel):
    skuMP: str = Field(..., description="SKU del componente (MP/INSUMO/INTERMEDIO)")
    cantidadPorBase: float = Field(..., ge=0)
    unidad: str
    mermaPct: Optional[float] = Field(0, ge=0)

class ProcesoIn(BaseModel):
    processCodigo: Optional[str] = None               # catálogo opcional
    procesoEspecialNombre: Optional[str] = None       # XOR con processCodigo
    procesoEspecialCosto: Optional[float] = Field(None, ge=0)

    @field_validator("procesoEspecialNombre")
    def validar_xor(cls, v, values):
        if not values.get("processCodigo") and not v and values.get("procesoEspecialCosto") is not None:
            raise ValueError("Si no hay processCodigo, procesoEspecialNombre es requerido.")
        return v

class RecetaVersionIn(BaseModel):
    numero: int = Field(..., ge=1)
    estado: EstadoVersion = "borrador"
    marcarVigente: bool = False
    fechaPublicacion: Optional[date] = None           # si None → backend la setea a hoy (UTC, solo fecha)
    publicadoPor: Optional[str] = None
    baseQty: float = Field(..., gt=0)
    unidadPT: str
    proceso: Optional[ProcesoIn] = None
    componentes: List[RecetaComponenteIn]

class CreateRecetaIn(BaseModel):
    skuPT: str
    vigenteVersion: Optional[int] = None              # opcional: setear vigente al crear
    version: RecetaVersionIn

class UpdateRecetaIn(BaseModel):
    vigenteVersion: Optional[int] = Field(None, description="Versión a marcar como vigente")
    updatedBy: Optional[str] = None

class UpdateRecetaVersionIn(BaseModel):
    estado: Optional[EstadoVersion] = None
    fechaPublicacion: Optional[date] = None
    publicadoPor: Optional[str] = None
    baseQty: Optional[float] = Field(None, gt=0)
    unidadPT: Optional[str] = None
    proceso: Optional[ProcesoIn] = None
    componentes: Optional[List[RecetaComponenteIn]] = None

class UpdateComponentesIn(BaseModel):
    componentes: List[RecetaComponenteIn]

# -------- Salidas (response) --------
class RecetaComponenteOut(BaseModel):
    productId: str
    cantidadPorBase: float
    unidad: str
    merma_pct: float

class VersionOut(BaseModel):
    version: int
    estado: EstadoVersion
    fechaPublicacion: date
    publicadoPor: Optional[str]
    base_qty: float
    unidad_PT: str
    processId: Optional[str] = None
    procesoEspecial_nombre: Optional[str] = None
    procesoEspecial_costo: Optional[float] = None
    componentes: List[RecetaComponenteOut]

class RecipeOut(BaseModel):
    id: str = Field(..., alias="_id")
    productPTId: str
    vigenteVersion: Optional[int] = None
    versiones: List[VersionOut]
    createdAt: datetime
    updatedAt: datetime

class UpdateRecetaNombreIn(BaseModel):
    nombre: str = Field(..., min_length=1, max_length=200, description="Nuevo nombre de la receta (PT)")