import os
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import re

import boto3
from botocore.config import Config
from pymongo import MongoClient, UpdateOne

from dotenv import load_dotenv
from app.core.config import settings

# Carga .env para disponer de credenciales AWS si no están exportadas
load_dotenv()

# -----------------------------------------
# Configuración básica (env + constantes)
# -----------------------------------------

APP_ENV = os.getenv("APP_ENV") or settings.APP_ENV or "dev"

MONGO_URI = os.getenv("MONGO_URI") or settings.MONGO_URI
MONGO_DB_NAME = os.getenv("MONGO_DB") or settings.MONGO_DB or "portal_sc_QA"
COLL_DECLAREPT = os.getenv("COLL_DECLAREPT", "declare_pt_events")
COLL_CONSUMIRVASOT = os.getenv("COLL_CONSUMIRVASOT", "consume_vasot_events")

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "surchile-softland")
# Prefijos S3
# Compat: si existía AWS_S3_PREFIX_DECLAREPT lo usamos como fallback.
AWS_S3_PREFIX_PLATFORM = (
    os.getenv("AWS_S3_PREFIX_PLATFORM")
    or os.getenv("AWS_S3_PREFIX_DECLAREPT")
    or "2/wms/SURCHILE1/PLATAFORMA/"
)
AWS_S3_PREFIX_PLATFORM_PROCECCED = os.getenv(
    "AWS_S3_PREFIX_PLATFORM_PROCECCED",
    f"{AWS_S3_PREFIX_PLATFORM.rstrip('/')}/PROCECCED/",
)
AWS_S3_PREFIX_PLATFORM_ERRORS = os.getenv(
    "AWS_S3_PREFIX_PLATFORM_ERRORS",
    f"{AWS_S3_PREFIX_PLATFORM.rstrip('/')}/PROCECCED/ERRORS/",
)

# Si en algún momento quisieras borrar el JSON una vez importado,
# puedes cambiar esto a True.
DELETE_JSON_AFTER_IMPORT = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("declarept_s3_sync")


# -----------------------------------------
# S3 & Mongo helpers
# -----------------------------------------


def get_s3_client():
    """
    Crea un cliente S3 usando las credenciales del entorno.
    """
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=AWS_REGION,
    )
    return session.client(
        "s3",
        config=Config(
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def get_mongo_collections():
    """
    Devuelve las colecciones de Mongo donde se guardan los eventos:
    - DECLARE_PT      -> COLL_DECLAREPT
    - CONSUMIR_VASOT -> COLL_CONSUMIRVASOT
    """
    if not MONGO_URI:
        raise RuntimeError("MONGO_URI no está definido en el entorno")

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    coll_declarept = db[COLL_DECLAREPT]
    coll_consumirvasot = db[COLL_CONSUMIRVASOT]
    return coll_declarept, coll_consumirvasot


def list_platform_objects(s3_client) -> List[str]:
    """
    Lista las keys de JSON en el prefijo PLATAFORMA/, ignorando PROCECCED/ y PROCECCED/ERRORS/.
    """
    logger.info(
        f"Listando objetos en s3://{AWS_S3_BUCKET}/{AWS_S3_PREFIX_PLATFORM}"
    )
    continuation_token: Optional[str] = None
    keys: List[str] = []

    while True:
        list_kwargs: Dict[str, Any] = {
            "Bucket": AWS_S3_BUCKET,
            "Prefix": AWS_S3_PREFIX_PLATFORM,
        }
        if continuation_token:
            list_kwargs["ContinuationToken"] = continuation_token

        resp = s3_client.list_objects_v2(**list_kwargs)
        contents = resp.get("Contents", [])

        for obj in contents:
            key = obj["Key"]

            # Ignorar "carpetas" y errores
            if key.endswith("/"):
                continue
            # Ignorar ya procesados o marcados como error
            if "/PROCECCED/" in key.upper():
                continue

            # Solo JSON
            if not key.lower().endswith(".json"):
                continue

            keys.append(key)

        if not resp.get("IsTruncated"):
            break

        continuation_token = resp.get("NextContinuationToken")

    logger.info(f"Encontrados {len(keys)} objetos JSON pendientes en PLATAFORMA/")
    return keys


def load_json_from_s3(s3_client, key: str) -> Dict[str, Any]:
    """
    Obtiene y parsea el JSON desde S3 para una key dada.
    """
    logger.info(f"Leyendo JSON desde S3: s3://{AWS_S3_BUCKET}/{key}")
    resp = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=key)
    body = resp["Body"].read()
    return json.loads(body)


def delete_s3_object(s3_client, key: str) -> None:
    """
    Elimina un objeto de S3.
    """
    logger.info(f"Eliminando JSON desde S3: s3://{AWS_S3_BUCKET}/{key}")
    s3_client.delete_object(Bucket=AWS_S3_BUCKET, Key=key)


def move_s3_object(s3_client, key: str, target_prefix: str) -> str:
    """
    Mueve (copy + delete) un objeto a un nuevo prefijo, preservando la parte relativa al prefijo base.
    Devuelve la key destino.
    """
    base_prefix = f"{AWS_S3_PREFIX_PLATFORM.rstrip('/')}/"
    relative = key[len(base_prefix) :] if key.startswith(base_prefix) else key.split("/")[-1]
    dest_key = f"{target_prefix.rstrip('/')}/{relative}"

    s3_client.copy_object(
        Bucket=AWS_S3_BUCKET,
        CopySource={"Bucket": AWS_S3_BUCKET, "Key": key},
        Key=dest_key,
    )
    s3_client.delete_object(Bucket=AWS_S3_BUCKET, Key=key)
    logger.info("Movido a s3://%s/%s", AWS_S3_BUCKET, dest_key)
    return dest_key


# -----------------------------------------
# Helpers de filtrado por idlpn
# -----------------------------------------

_IDLPN_REGEX = re.compile(r"_(?P<idlpn>[^_/]+)\.json$", re.IGNORECASE)


def extract_idlpn_from_key(key: str) -> Optional[str]:
    """
    Extrae el idlpn desde el nombre del archivo: DECLAREPT_OT_IDLPN.json.
    Devuelve None si no cumple el patrón.
    """
    match = _IDLPN_REGEX.search(key)
    if not match:
        return None
    return match.group("idlpn")


def existing_idlpns(coll_declarept, coll_consumirvasot, idlpns: List[str]) -> set[str]:
    """
    Obtiene los idlpn ya presentes en ambas colecciones para saltar descargas repetidas.
    """
    if not idlpns:
        return set()
    flt = {"idlpn": {"$in": idlpns}}
    found = set(coll_declarept.distinct("idlpn", flt))
    found |= set(coll_consumirvasot.distinct("idlpn", flt))
    return found


# -----------------------------------------
# Lógica de importación a Mongo
# -----------------------------------------


def build_upsert_filter(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Define cómo se identifica de forma única un evento para hacer upsert.
    Usamos stage + work_order + document_number + idlpn.
    """
    return {
        "stage": event.get("stage"),
        "work_order": event.get("work_order"),
        "document_number": event.get("document_number"),
        "idlpn": event.get("idlpn"),
    }


def normalize_event(event: Dict[str, Any], s3_key: str) -> Dict[str, Any]:
    """
    Asegura algunos campos estándar para guardar en Mongo.
    """
    now_utc = datetime.now(timezone.utc)

    event_copy = dict(event)  # para no mutar el original
    event_copy["source_s3_key"] = s3_key
    event_copy.setdefault("ingested_at", now_utc)
    # Si viene tipoEvento desde el JSON (DECLARE_PT / CONSUMIR_VASOT), se respeta.
    event_copy.setdefault("tipoEvento", "DECLARE_PT")

    # Por si en algún ambiente falta stage
    event_copy.setdefault("stage", APP_ENV)

    return event_copy


def sync_platform_events() -> None:
    """
    Proceso principal:
    - Lista los JSON en S3 (PLATAFORMA/)
    - Los lee y normaliza
    - Los guarda en Mongo con upsert:
        * DECLARE_PT      -> colección COLL_DECLAREPT
        * CONSUMIR_VASOT  -> colección COLL_CONSUMIRVASOT
    - Mueve los archivos procesados a PROCECCED/ o PROCECCED/ERRORS/
    """
    started_at = time.monotonic()
    logger.info("Iniciando sync_platform_events()")

    s3 = get_s3_client()
    coll_declarept, coll_consumirvasot = get_mongo_collections()

    keys = list_platform_objects(s3)
    if not keys:
        logger.info("No hay JSON de plataforma para procesar.")
        return

    ops_declarept: List[UpdateOne] = []
    ops_consumirvasot: List[UpdateOne] = []

    # Mapa key -> idlpn para poder saltar archivos ya ingeridos
    key_to_idlpn: Dict[str, Optional[str]] = {k: extract_idlpn_from_key(k) for k in keys}
    candidate_idlpns = [v for v in key_to_idlpn.values() if v]
    already = existing_idlpns(coll_declarept, coll_consumirvasot, candidate_idlpns)
    if already:
        logger.info("Saltando %s archivos ya ingeridos (idlpn)", len(already))

    for key in keys:
        target_prefix = AWS_S3_PREFIX_PLATFORM_PROCECCED
        moved = False
        try:
            idlpn = key_to_idlpn.get(key)
            if idlpn and idlpn in already:
                logger.info("Archivo ya procesado (idlpn=%s), moviendo a PROCECCED: %s", idlpn, key)
                move_s3_object(s3, key, target_prefix)
                moved = True
                continue

            event = load_json_from_s3(s3, key)
            normalized = normalize_event(event, key)
            upsert_filter = build_upsert_filter(normalized)

            tipo = (normalized.get("tipoEvento") or "").upper()

            if tipo == "DECLARE_PT":
                ops_declarept.append(
                    UpdateOne(upsert_filter, {"$set": normalized}, upsert=True)
                )
            elif tipo == "CONSUMIR_VASOT":
                ops_consumirvasot.append(
                    UpdateOne(upsert_filter, {"$set": normalized}, upsert=True)
                )
            else:
                logger.warning(
                    f"Ignorando JSON con tipoEvento desconocido "
                    f"({tipo}) en s3://{AWS_S3_BUCKET}/{key}"
                )

        except Exception as e:
            logger.error(
                f"Error procesando JSON s3://{AWS_S3_BUCKET}/{key}: {e}"
            )
            target_prefix = AWS_S3_PREFIX_PLATFORM_ERRORS
        else:
            target_prefix = AWS_S3_PREFIX_PLATFORM_PROCECCED
        finally:
            if not moved:
                try:
                    move_s3_object(s3, key, target_prefix)
                except Exception as move_exc:
                    logger.exception(
                        "No se pudo mover s3://%s/%s a %s: %s",
                        AWS_S3_BUCKET,
                        key,
                        target_prefix,
                        move_exc,
                    )

    # Ejecutar bulk para DECLARE_PT
    if ops_declarept:
        logger.info(
            f"Ejecutando bulk_write DECLARE_PT con {len(ops_declarept)} operaciones..."
        )
        result = coll_declarept.bulk_write(ops_declarept, ordered=False)
        logger.info(
            f"[DECLARE_PT] Upserts: {result.upserted_count}, "
            f"Modificados: {result.modified_count}, "
            f"Coincidencias: {result.matched_count}"
        )

    # Ejecutar bulk para CONSUMIR_VASOT
    if ops_consumirvasot:
        logger.info(
            f"Ejecutando bulk_write CONSUMIR_VASOT con {len(ops_consumirvasot)} operaciones..."
        )
        result = coll_consumirvasot.bulk_write(ops_consumirvasot, ordered=False)
        logger.info(
            f"[CONSUMIR_VASOT] Upserts: {result.upserted_count}, "
            f"Modificados: {result.modified_count}, "
            f"Coincidencias: {result.matched_count}"
        )

    elapsed = time.monotonic() - started_at
    logger.info("sync_platform_events() finalizado en %.2fs", elapsed)


# -----------------------------------------
# Punto de entrada
# -----------------------------------------

if __name__ == "__main__":
    try:
        sync_platform_events()
    except Exception as exc:
        logger.exception(f"Error fatal en sync_platform_events: {exc}")
