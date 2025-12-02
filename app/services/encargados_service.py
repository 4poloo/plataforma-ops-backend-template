from __future__ import annotations

from typing import Any, Dict, List, Optional

from bson import ObjectId

from app.db.repositories import encargados_repo
from app.models.encargados import EncargadoCreate, EncargadoUpdate


def _map_encargado(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "_id": str(doc["_id"]),
        "nombre": doc.get("nombre"),
        "linea": doc.get("linea"),
        "predeterminado": bool(doc.get("predeterminado", False)),
    }


async def list_encargados(
    db,
    *,
    linea: Optional[str] = None,
    nombre: Optional[str] = None,
    limit: int = 100,
    skip: int = 0,
) -> List[Dict[str, Any]]:
    filtro: Dict[str, Any] = {}
    if linea:
        filtro["linea"] = linea.strip()
    if nombre:
        filtro["nombre"] = {"$regex": nombre.strip(), "$options": "i"}
    docs = await encargados_repo.find_all(filtro=filtro, limit=limit, skip=skip, db=db)
    return [_map_encargado(doc) for doc in docs]


async def create_encargado(db, payload: EncargadoCreate) -> Dict[str, Any]:
    existing = await encargados_repo.find_by_nombre_linea(payload.nombre, payload.linea, db=db)
    if existing:
        raise ValueError("Ya existe un encargado con ese nombre y línea")
    doc = {
        "nombre": payload.nombre,
        "linea": payload.linea,
        "predeterminado": bool(payload.predeterminado),
    }
    inserted = await encargados_repo.insert_encargado(doc, db=db)
    return _map_encargado(inserted)


async def update_encargado(db, encargado_id: str, payload: EncargadoUpdate) -> Dict[str, Any]:
    current = await encargados_repo.find_by_id(encargado_id, db=db)
    if not current:
        raise ValueError("Encargado no encontrado")

    update_data: Dict[str, Any] = {}
    if payload.nombre is not None:
        update_data["nombre"] = payload.nombre
    if payload.linea is not None:
        update_data["linea"] = payload.linea
    if payload.predeterminado is not None:
        update_data["predeterminado"] = bool(payload.predeterminado)

    if update_data:
        nombre = update_data.get("nombre", current.get("nombre"))
        linea = update_data.get("linea", current.get("linea"))
        conflict = await encargados_repo.find_by_nombre_linea(
            nombre,
            linea,
            exclude_id=current["_id"],
            db=db,
        )
        if conflict:
            raise ValueError("Ya existe otro encargado con ese nombre y línea")

    updated = await encargados_repo.update_encargado(encargado_id, update_data, db=db)
    if not updated:
        raise ValueError("No fue posible actualizar el encargado")
    return _map_encargado(updated)
