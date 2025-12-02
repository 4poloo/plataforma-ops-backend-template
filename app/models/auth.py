from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from app.models.users import UserOut


class LoginRequest(BaseModel):
    alias: str = Field(..., min_length=1, description="Alias del usuario.")
    password: str = Field(..., min_length=1, description="Contraseña en texto plano.")


class LoginResponse(BaseModel):
    success: bool
    message: str
    user: Optional[UserOut] = None
