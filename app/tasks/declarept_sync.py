import asyncio
import logging
import os

from app.utils import declarept_s3_sync

logger = logging.getLogger(__name__)

_task: asyncio.Task | None = None

# Default every 5 minutes unless overridden via env.
_INTERVAL_SECONDS = max(
    60,
    int(os.getenv("DECLAREPT_SYNC_INTERVAL_SECONDS", "300") or 300),
)


async def _sync_loop():
    while True:
        try:
            # Run the blocking sync in a thread to avoid blocking the event loop.
            await asyncio.to_thread(declarept_s3_sync.sync_platform_events)
        except Exception:
            logger.exception("Error ejecutando sync_platform_events")
        await asyncio.sleep(_INTERVAL_SECONDS)


def start_sync_task() -> asyncio.Task:
    """Inicia la tarea en background si no existe."""
    global _task
    if _task is None or _task.done():
        _task = asyncio.create_task(_sync_loop())
    return _task


async def stop_sync_task():
    global _task
    if _task is None:
        return
    _task.cancel()
    try:
        await _task
    except asyncio.CancelledError:
        pass
    _task = None
