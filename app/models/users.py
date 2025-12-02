# app/schemas/users.py
from __future__ import annotations

from pydantic import BaseModel, Field, EmailStr, validator
from typing import Optional, Literal, List, Tuple, cast
from datetime import datetime
from typing import get_args

# ---- Tipos base
UserRole = Literal[
    "admin",
    "gerente",
    "jefe_produccion",
    "jefe_planta",
    "operador",
    "jefe_bodega",
    "jefe_picking",
    "informatica",
    "trabajador",
]
UserStatus = Literal["active", "disabled"]

_ROLE_ALLOWED: Tuple[str, ...] = cast(Tuple[str, ...], get_args(UserRole))
_ROLE_ALLOWED_SET = set(_ROLE_ALLOWED)
_ROLE_ALIASES = {"administrador": "admin"}


def normalize_role_value(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip().lower().replace(" ", "_")
    normalized = _ROLE_ALIASES.get(normalized, normalized)
    if normalized not in _ROLE_ALLOWED_SET:
        allowed = ", ".join(sorted(_ROLE_ALLOWED_SET))
        raise ValueError(f"Rol no válido. Roles permitidos: {allowed}")
    return normalized


# ---- Requests
class UserCreateIn(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8, description="Se almacenará hasheada")
    nombre: str
    alias: str
    apellido: Optional[str] = None
    role: UserRole = "trabajador"

    @validator("role", pre=True, always=True)
    def _normalize_role(cls, value: Optional[str]) -> str:
        normalized = normalize_role_value(value) or "trabajador"
        return normalized

class UserUpdateIn(BaseModel):
    nombre: Optional[str] = None
    apellido: Optional[str] = None
    alias: Optional[str] = None
    role: Optional[UserRole] = None
    status: Optional[UserStatus] = None

    @validator("role", pre=True)
    def _normalize_role(cls, value: Optional[str]) -> Optional[str]:
        return normalize_role_value(value)

class ChangePasswordIn(BaseModel):
    passwordActual: str
    passwordNueva: str = Field(..., min_length=8)

# ---- Responses
class UserOut(BaseModel):
    id: str = Field(..., description="String de ObjectId")
    email: EmailStr
    nombre: str
    alias: str
    apellido: Optional[str] = None
    role: UserRole
    status: UserStatus
    createdAt: datetime
    updatedAt: datetime

class UsersListOut(BaseModel):
    items: List[UserOut]
    total: int
    skip: int
    limit: int
