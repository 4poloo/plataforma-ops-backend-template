from __future__ import annotations

from fastapi import APIRouter, Depends, status

from app.db.mongo import get_db
from app.models.auth import LoginRequest, LoginResponse
from app.services import auth_service

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login", response_model=LoginResponse, status_code=status.HTTP_200_OK)
async def login(body: LoginRequest, db=Depends(get_db)):
    """
    Verifica alias y contraseña contra la colección de usuarios.

    Devuelve un mensaje descriptivo del resultado y, si es correcto, el usuario
    serializado (`UserOut`).
    """
    success, message, user = await auth_service.authenticate(
        db, body.alias, body.password
    )
    return {"success": success, "message": message, "user": user}
