from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from bson import ObjectId

from app.core.security import verify_password
from app.db.repositories import users_repo


def _oid_str(value: ObjectId | None) -> Optional[str]:
    return str(value) if value is not None else None


def _map_user_out(doc: Dict[str, Any]) -> Dict[str, Any]:
    audit = doc.get("audit", {})
    return {
        "id": _oid_str(doc.get("_id")),
        "email": doc.get("email"),
        "nombre": doc.get("nombre"),
        "alias": doc.get("alias"),
        "apellido": doc.get("apellido"),
        "role": doc.get("role"),
        "status": doc.get("status", "active"),
        "createdAt": audit.get("createdAt"),
        "updatedAt": audit.get("updatedAt"),
    }


async def authenticate(
    db,
    alias: str,
    password: str,
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    alias_clean = alias.strip()
    if not alias_clean:
        return False, "Alias requerido.", None

    user = await users_repo.find_one_by_alias(alias_clean, db)
    if not user:
        return False, "Usuario no encontrado.", None

    if user.get("status", "active") != "active":
        return False, "Usuario deshabilitado.", None

    hashed_password = user.get("passwordHash", "")
    if not hashed_password or not verify_password(password, hashed_password):
        return False, "Contraseña incorrecta.", None

    return True, "Autenticación exitosa.", _map_user_out(user)
