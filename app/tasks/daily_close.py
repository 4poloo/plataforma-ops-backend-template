import asyncio
import logging
from datetime import datetime, time, timedelta, timezone

from app.db.mongo import get_db
from app.services import gestion_ot_prod

logger = logging.getLogger(__name__)

_task: asyncio.Task | None = None


def _seconds_until_next_midnight() -> float:
    now = datetime.now(timezone.utc)
    tomorrow = now.date() + timedelta(days=1)
    next_midnight = datetime.combine(tomorrow, time.min, tzinfo=timezone.utc)
    delta = (next_midnight - now).total_seconds()
    # Evita sleep muy cortos si el reloj se desfasa
    return max(delta, 60.0)


async def _close_previous_day_loop():
    while True:
        try:
            db = get_db()
            closed = await gestion_ot_prod.close_previous_day_entries(db)
            logger.info(
                "Cierre diario de OT: gestion_ot_prod=%s | work_orders=%s",
                closed.get("gestion_ot_prod"),
                closed.get("work_orders"),
            )
        except Exception:
            logger.exception("Error al cerrar OT del día anterior")
        await asyncio.sleep(_seconds_until_next_midnight())


def start_close_task() -> asyncio.Task:
    """Inicia la tarea en background si no existe."""
    global _task
    if _task is None or _task.done():
        _task = asyncio.create_task(_close_previous_day_loop())
    return _task


async def stop_close_task():
    global _task
    if _task is None:
        return
    _task.cancel()
    try:
        await _task
    except asyncio.CancelledError:
        pass
    _task = None
