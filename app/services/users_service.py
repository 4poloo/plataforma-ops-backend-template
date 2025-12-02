# app/services/users_service.py
from __future__ import annotations

from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timezone
from bson import ObjectId

from app.core.security import hash_password, verify_password
from app.db.repositories import users_repo
from app.models.users import UserCreateIn, UserUpdateIn, UserRole, UserStatus

def _oid_str(oid: ObjectId | None) -> Optional[str]:
    return str(oid) if oid is not None else None

def _map_out(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": _oid_str(doc["_id"]),
        "email": doc["email"],
        "nombre": doc.get("nombre"),
        "alias": doc.get("alias"),
        "apellido": doc.get("apellido"),
        "role": doc.get("role"),
        "status": doc.get("status", "active"),
        "createdAt": doc["audit"]["createdAt"],
        "updatedAt": doc["audit"]["updatedAt"],
    }

async def create_user(db, payload: UserCreateIn) -> Dict[str, Any]:
    alias_clean = payload.alias.strip()
    email_clean = payload.email.strip()

    if await users_repo.find_one_by_alias(alias_clean, db):
        raise ValueError("alias ya existe")

    # email único (case-insensitive)
    if await users_repo.find_by_email(email_clean, db):
        raise ValueError("email ya registrado")

    now = datetime.now(timezone.utc)
    password_hash = hash_password(payload.password)

    doc = {
        "email": email_clean,
        "email_ci": email_clean.lower(),
        "alias": alias_clean,
        "alias_ci": alias_clean.lower(),
        "passwordHash": password_hash,
        "nombre": payload.nombre.strip(),
        "apellido": (payload.apellido or "").strip() or None,
        "role": payload.role,
        "status": "active",
        "audit": {"createdAt": now, "updatedAt": now, "createdBy": "admin"},
    }
    created = await users_repo.insert_user(doc, db)
    return _map_out(created)

async def update_user(db, user_id: str, body: UserUpdateIn) -> Dict[str, Any]:
    user = await users_repo.find_by_id(user_id, db)
    if not user:
        raise ValueError("Usuario no encontrado")

    set_fields: Dict[str, Any] = {}
    if body.nombre is not None:
        set_fields["nombre"] = body.nombre.strip()
    if body.alias is not None:
        set_fields["alias"] = body.alias.strip()
        set_fields["alias_ci"] = body.alias.strip().lower()
    if body.apellido is not None:
        set_fields["apellido"] = (body.apellido or "").strip() or None
    if body.role is not None:
        set_fields["role"] = body.role
    if body.status is not None:
        set_fields["status"] = body.status
    set_fields["audit.updatedAt"] = datetime.now(timezone.utc)

    updated = await users_repo.update_user_by_id(user_id, set_fields, db)
    return _map_out(updated)

async def get_user_by_id(db, user_id: str) -> Dict[str, Any]:
    user = await users_repo.find_by_id(user_id, db)
    if not user:
        raise ValueError("Usuario no encontrado")
    return _map_out(user)

async def get_user_by_alias(db, user_alias: str) -> Dict[str, Any]:
    users = await users_repo.find_by_alias(user_alias, db)
    if not users:
        raise ValueError("Usuario no encontrado")
    return [ _map_out(d) for d in users ]

async def get_user_by_email(db, email: str) -> Dict[str, Any]:
    user = await users_repo.find_by_email(email, db)
    if not user:
        raise ValueError("Usuario no encontrado")
    return _map_out(user)

async def list_users(
    db,
    *,
    q: str | None,
    status: UserStatus | None,
    role: UserRole | None,
    skip: int,
    limit: int,
) -> Tuple[list[Dict[str, Any]], int]:
    filtro: Dict[str, Any] = {}
    if q:
        filtro["$or"] = [
            {"email": {"$regex": q, "$options": "i"}},
            {"nombre": {"$regex": q, "$options": "i"}},
            {"apellido": {"$regex": q, "$options": "i"}},
        ]
    if status:
        filtro["status"] = status
    if role:
        filtro["role"] = role

    docs = await users_repo.list_users(filtro=filtro, skip=skip, limit=limit, sort=[("audit.createdAt", -1)], db=db)
    total = await users_repo.count_users(filtro=filtro, db=db)
    return [ _map_out(d) for d in docs ], total

async def change_password(db, user_id: str, password_actual: str, password_nueva: str) -> Dict[str, Any]:
    user = await users_repo.find_by_id(user_id, db)
    if not user:
        raise ValueError("Usuario no encontrado")

    # valida hash actual
    if not verify_password(password_actual, user.get("passwordHash", "")):
        raise ValueError("La contraseña actual no es válida")
    
    new_hash = hash_password(password_nueva)
    updated = await users_repo.set_password_hash(user_id, new_hash, db)
    return _map_out(updated)
