# app/core/security.py
from __future__ import annotations
from passlib.context import CryptContext

# Soporta verificación de hashes antiguos "bcrypt"
# y genera nuevos hashes con "bcrypt_sha256" (sin límite de 72 bytes).
pwd_context = CryptContext(
    schemes=["bcrypt_sha256", "bcrypt"],
    default="bcrypt_sha256",
    deprecated="auto",
)

def hash_password(plain: str) -> str:
    # passlib se encarga de pre-hashear con SHA-256 y luego bcrypt
    return pwd_context.hash(plain)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)
