"""
Se debe crear una funcion que normalice los datos para que ingresen bien con pydantic
tambien el tema de las contrasenas (Largo), ademas de eso que normalice el rol (usar list en Front)
normaliza en Back. todo lo que ingrese este en .lower()
"""
from __future__ import annotations

from typing import Optional, List, cast
from fastapi import APIRouter, HTTPException, Query, Depends, status

from app.db.mongo import get_db
from app.models.users import (
    UserCreateIn, UserUpdateIn, ChangePasswordIn,
    UserOut, UsersListOut, UserRole, UserStatus, normalize_role_value
)
from app.services import users_service

router = APIRouter(prefix="/users", tags=["users"])

# --------- Crear usuario ---------
@router.post("", response_model=UserOut, status_code=status.HTTP_201_CREATED)
async def create_user(body: UserCreateIn, db=Depends(get_db)):
    try:
        return await users_service.create_user(db, body)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

# --------- Listar usuarios ---------
@router.get("", response_model=UsersListOut)
async def list_users(
    q: Optional[str] = Query(None, description="Busca en email/nombre/apellido"),
    status: Optional[UserStatus] = Query(None),
    role: Optional[str] = Query(
        None,
        description="Filtra por rol. Acepta valores como admin, administrador, gerente, jefe_produccion, jefe_planta, operador, jefe_bodega, jefe_picking, informatica, trabajador.",
    ),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db=Depends(get_db),
):
    normalized_role: Optional[UserRole] = None
    if role is not None:
        try:
            normalized_value = normalize_role_value(role)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
        if normalized_value is not None:
            normalized_role = cast(UserRole, normalized_value)

    items, total = await users_service.list_users(db, q=q, status=status, role=normalized_role, skip=skip, limit=limit)
    return {"items": items, "total": total, "skip": skip, "limit": limit}

# --------- Obtener por id ---------
@router.get("/{user_id}", response_model=UserOut)
async def get_user_by_id(user_id: str, db=Depends(get_db)):
    try:
        return await users_service.get_user_by_id(db, user_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    
# --------- Obtener por alias ---------
@router.get("/by-alias/{user_alias}", response_model=List[UserOut])
async def get_user_by_id(user_alias: str, db=Depends(get_db)):
    try:
        return await users_service.get_user_by_alias(db, user_alias)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

# --------- Obtener por email ---------
@router.get("/by-email/{email}", response_model=UserOut)
async def get_user_by_email(email: str, db=Depends(get_db)):
    try:
        return await users_service.get_user_by_email(db, email)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

# --------- Actualizar perfil/rol/estado ---------
@router.put("/{user_id}", response_model=UserOut)
async def update_user(user_id: str, body: UserUpdateIn, db=Depends(get_db)):
    try:
        return await users_service.update_user(db, user_id, body)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

# --------- Cambiar contraseña ---------
@router.post("/{user_id}/change-password", response_model=UserOut)
async def change_password(user_id: str, body: ChangePasswordIn, db=Depends(get_db)):
    try:
        return await users_service.change_password(db, user_id, body.passwordActual, body.passwordNueva)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
