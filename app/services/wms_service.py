from __future__ import annotations

from typing import Any, Dict, Tuple
import asyncio
import base64
import json
import os
import logging
from datetime import datetime, timedelta, timezone
from urllib import request, error

from app.core.config import settings
from app.models.work_orders import (
    WorkOrderIntegrationRequest,
    WorkOrderIntegrationResponse,
    WorkOrderStatusOut,
)


class WMSIntegrationError(Exception):
    def __init__(self, status_code: int, body: Any):
        self.status_code = status_code
        self.body = body
        message = f"Error al integrar con WMS (status={status_code})"
        super().__init__(message)


logger = logging.getLogger(__name__)

_QA_ALIAS = {"qa", "quality", "testing", "test"}
_PROD_ALIAS = {"prod", "production", "live"}
_STATUS_MAP = {
    "CREADA": "CREADA",
    "DESPACHADA": "CERRADA",
    "RECHAZADO CLIENTE": "ERROR",
}
_TOKEN_TTL = timedelta(hours=23)
_TOKEN_CACHE: Dict[str, Tuple[str, datetime]] = {}


def _build_wms_url() -> str:
    base = _get_setting("WMS_URL")
    if not base:
        raise ValueError("WMS_URL no está configurado")
    base = base.rstrip("/")
    return f"{base}/work_order_JSON/integration/work-orders/json"


def _normalize_env(target_env: str | None) -> str:
    return (target_env or settings.APP_ENV or "prod").lower()


def _build_request(
    url: str,
    json_payload: Dict[str, Any],
    auth: Tuple[str, str] | None = None,
    bearer_token: str | None = None,
):
    data_bytes = json.dumps(json_payload).encode("utf-8")
    req = request.Request(url, data=data_bytes, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    if auth:
        token = base64.b64encode(f"{auth[0]}:{auth[1]}".encode("utf-8")).decode("ascii")
        req.add_header("Authorization", f"Basic {token}")
    elif bearer_token:
        req.add_header("Authorization", f"Bearer {bearer_token}")
    return req


def _parse_body(raw: bytes) -> Any:
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return raw.decode("utf-8", errors="replace")


async def send_work_orders(payload: WorkOrderIntegrationRequest) -> WorkOrderIntegrationResponse:

    url = _build_wms_url()
    user = _get_setting("WMS_USER")
    password = _get_setting("WMS_PASS")
    auth_tuple = None
    if user and password:
        auth_tuple = (user, password)

    request_body = {
        "source": payload.source,
        "payload": [item.model_dump(mode="json") for item in payload.payload],
    }
    logger.info("Enviando payload a WMS | url=%s | body=%s", url, json.dumps(request_body, ensure_ascii=False))

    timeout_value = getattr(settings, "WMS_TIMEOUT_SECONDS", None)
    if not timeout_value:
        timeout_value = os.getenv("WMS_TIMEOUT_SECONDS", "30")
    if not isinstance(timeout_value, int):
        try:
            timeout_value = int(timeout_value)
        except (TypeError, ValueError):
            timeout_value = 30
    timeout_seconds = timeout_value or 30
    if timeout_seconds <= 0:
        timeout_seconds = 30

    req = _build_request(url, request_body, auth_tuple)

    def _do_request():
        try:
            with request.urlopen(req, timeout=timeout_seconds) as resp:
                content = resp.read()
                status = resp.status
        except error.HTTPError as exc:
            raise WMSIntegrationError(exc.code, _parse_body(exc.read())) from exc
        except error.URLError as exc:
            raise WMSIntegrationError(503, f"Error de conexión con WMS: {exc}") from exc
        return status, content

    status_code, raw_body = await asyncio.to_thread(_do_request)
    body = _parse_body(raw_body)

    if status_code >= 400:
        raise WMSIntegrationError(status_code, body)

    logger.info("Respuesta WMS | status=%s | body=%s", status_code, body)

    return WorkOrderIntegrationResponse(status_code=status_code, body=body)


def _build_status_url(target_env: str | None) -> str:
    env = _normalize_env(target_env)
    if env in _PROD_ALIAS:
        base = _get_setting("WMS_QUERY_URL_PROD") or _get_setting("WMS_QUERY_URL_QA")
    else:
        base = _get_setting("WMS_QUERY_URL_QA") or _get_setting("WMS_QUERY_URL_PROD")
    if not base:
        raise ValueError("URL de consulta WMS no está configurada")
    return base


def _build_login_url(target_env: str | None) -> str:
    env = _normalize_env(target_env)
    if env in _PROD_ALIAS:
        base = _get_setting("WMS_LOGIN_URL_PROD") or _get_setting("WMS_LOGIN_URL_QA")
    else:
        base = _get_setting("WMS_LOGIN_URL_QA") or _get_setting("WMS_LOGIN_URL_PROD")
    if not base:
        raise ValueError("URL de login WMS no está configurada")
    return base


async def _fetch_token(target_env: str | None) -> Tuple[str, datetime]:
    url = _build_login_url(target_env)
    user = _get_setting("WMS_USER")
    password = _get_setting("WMS_PASS")
    if not user or not password:
        raise ValueError("Credenciales WMS no configuradas")

    request_body = {"usuario": user, "password": password}
    masked_body = {"usuario": user, "password": "***"}
    logger.info(
        "Solicitando token WMS | env=%s | url=%s | body=%s",
        _normalize_env(target_env),
        url,
        json.dumps(masked_body, ensure_ascii=False),
    )

    timeout_value = getattr(settings, "WMS_TIMEOUT_SECONDS", None)
    if not timeout_value:
        timeout_value = os.getenv("WMS_TIMEOUT_SECONDS", "30")
    if not isinstance(timeout_value, int):
        try:
            timeout_value = int(timeout_value)
        except (TypeError, ValueError):
            timeout_value = 30
    timeout_seconds = max(int(timeout_value), 1)

    req = _build_request(url, request_body)

    def _do_request():
        try:
            with request.urlopen(req, timeout=timeout_seconds) as resp:
                content = resp.read()
                status = resp.status
        except error.HTTPError as exc:
            raise WMSIntegrationError(exc.code, _parse_body(exc.read())) from exc
        except error.URLError as exc:
            raise WMSIntegrationError(503, f"Error de conexión con WMS: {exc}") from exc
        return status, content

    status_code, raw_body = await asyncio.to_thread(_do_request)
    body = _parse_body(raw_body)

    if status_code >= 400:
        raise WMSIntegrationError(status_code, body)

    if not isinstance(body, dict):
        raise ValueError("Respuesta de login WMS inválida")

    token = body.get("token")
    error_msg = body.get("error")
    if error_msg:
        raise ValueError(f"Error de login WMS: {error_msg}")
    if not token:
        raise ValueError("Login WMS no devolvió token")

    expires_at = datetime.now(timezone.utc) + _TOKEN_TTL
    logger.info("Token WMS obtenido | env=%s | exp=%s", _normalize_env(target_env), expires_at.isoformat())
    return token, expires_at


async def _get_token(target_env: str | None) -> str:
    env_key = _normalize_env(target_env)
    cached = _TOKEN_CACHE.get(env_key)
    now = datetime.now(timezone.utc)
    if cached:
        token, expires = cached
        if expires > now + timedelta(minutes=5):
            return token
    token, expires = await _fetch_token(target_env)
    _TOKEN_CACHE[env_key] = (token, expires)
    return token


async def query_work_order_status(code: str, *, target_env: str | None = None) -> WorkOrderStatusOut:
    code_input = str(code or "").strip()
    if not code_input:
        raise ValueError("El código de OT es requerido")

    normalized_code = code_input.upper()
    if not normalized_code.startswith("OT-"):
        normalized_code = f"OT-{normalized_code}"
    code = normalized_code

    url = _build_status_url(target_env)
    user = _get_setting("WMS_USER")
    password = _get_setting("WMS_PASS")
    if not user or not password:
        raise ValueError("Credenciales WMS no configuradas")

    request_body = {
        "usuario": user,
        "password": password,
        "listaOS": [{"idOs": code}],
    }
    masked_body = {
        "usuario": user,
        "password": "***",
        "listaOS": [{"idOs": code}],
    }
    logger.info(
        "Consultando estado OT | env=%s | url=%s | body=%s",
        (target_env or settings.APP_ENV or "prod"),
        url,
        json.dumps(masked_body, ensure_ascii=False),
    )

    timeout_value = getattr(settings, "WMS_TIMEOUT_SECONDS", None)
    if not timeout_value:
        timeout_value = os.getenv("WMS_TIMEOUT_SECONDS", "30")
    if not isinstance(timeout_value, int):
        try:
            timeout_value = int(timeout_value)
        except (TypeError, ValueError):
            timeout_value = 30
    timeout_seconds = timeout_value or 30
    if timeout_seconds <= 0:
        timeout_seconds = 30

    env_key = _normalize_env(target_env)

    def _request_once(token: str):
        req_local = _build_request(url, request_body, auth=None, bearer_token=token)

        try:
            with request.urlopen(req_local, timeout=timeout_seconds) as resp:
                content = resp.read()
                status = resp.status
        except error.HTTPError as exc:
            raise WMSIntegrationError(exc.code, _parse_body(exc.read())) from exc
        except error.URLError as exc:
            raise WMSIntegrationError(503, f"Error de conexión con WMS: {exc}") from exc
        return status, content

    attempts = 0
    while True:
        bearer_token = await _get_token(target_env)
        try:
            status_code, raw_body = await asyncio.to_thread(_request_once, bearer_token)
        except WMSIntegrationError as exc:
            if exc.status_code == 401 and attempts == 0:
                _TOKEN_CACHE.pop(env_key, None)
                attempts += 1
                logger.info("Token WMS expirado, intentando refrescar | env=%s", env_key)
                continue
            raise
        if status_code == 401 and attempts == 0:
            _TOKEN_CACHE.pop(env_key, None)
            attempts += 1
            logger.info("Token WMS inválido, intentando refrescar | env=%s", env_key)
            continue
        break

    body = _parse_body(raw_body)

    if status_code >= 400:
        raise WMSIntegrationError(status_code, body)

    logger.info("Estado OT recibido | status=%s | body=%s", status_code, body)

    if not isinstance(body, dict):
        raise ValueError("Respuesta de WMS inválida: formato inesperado")

    error_msg = body.get("error")
    if error_msg:
        raise ValueError(f"Error desde WMS: {error_msg}")

    lista = body.get("listaOs") or []
    match = next(
        (item for item in lista if str(item.get("idOs", "")).upper() == code),
        lista[0] if lista else None,
    )
    if not match:
        raise ValueError("WMS no devolvió datos para la OT solicitada")

    raw_state = str(match.get("estado") or "").strip()
    mapped_state = _STATUS_MAP.get(raw_state.upper(), "EN_PROCESO")

    return WorkOrderStatusOut(
        code=str(match.get("idOs") or code),
        state=mapped_state,
        state_raw=raw_state,
        site=match.get("sitio"),
        created_at=match.get("fechaCreacion"),
        updated_at=match.get("fechaultimamod"),
    )


def _get_setting(name: str, default: str = "") -> str:
    value = getattr(settings, name, None)
    if value is None:
        value = default
    if isinstance(value, str):
        value = value.strip()
    return value or os.getenv(name, default).strip()
