from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging
from app.core.config import settings
from app.db.mongo import connect, close, get_db
from app.db.repositories import (
    encargados_repo,
    gestion_ot_prod_repo,
    logs_repo,
    products_repo,
    work_orders_repo,
)
from app.api.v1 import auth, counters, encargados, gestion_produccion, logs, products, recipes, users, work_orders, dashboards
from fastapi.middleware.cors import CORSMiddleware
from app.tasks import daily_close, declarept_sync


@asynccontextmanager
async def init(app:FastAPI):
    await connect()
    await products_repo.ensure_indexes()
    await work_orders_repo.ensure_indexes()
    await gestion_ot_prod_repo.ensure_indexes()
    await encargados_repo.ensure_indexes()
    await logs_repo.ensure_indexes()
    closer_task = daily_close.start_close_task()
    declarept_task = declarept_sync.start_sync_task()
    yield
    await daily_close.stop_close_task()
    await declarept_sync.stop_sync_task()
    await close()

app = FastAPI(
    title="Backend SC-PWP",
    version="0.1.0",
    lifespan=init,
    openapi_url="/openapi.json"
)

logging.basicConfig(
    level=getattr(logging, (settings.LOG_LEVEL or "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

# En producción NO uses "*" si allow_credentials=True.
ALLOWED_ORIGINS = [
    "http://localhost:5173",      # Vite (dev)
    "http://127.0.0.1:5173",      # añadido: Vite puede usar 127.0.0.1
    "http://localhost:3000",      # otra app local si aplica
    "https://portal.surchile.cl", # tu dominio real
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,                # lista explícita de orígenes
    allow_credentials=True,                       # cookies/Authorization
    allow_methods=["GET","POST","PUT","PATCH","DELETE","OPTIONS"],
    allow_headers=["*"],                          # relajado: acepta cualquier header del preflight
    expose_headers=["X-Total-Count"],
    max_age=86400,
)

app.include_router(products.router, prefix=settings.API_V1_PREFIX)
app.include_router(recipes.router, prefix=settings.API_V1_PREFIX)
app.include_router(users.router, prefix=settings.API_V1_PREFIX)
app.include_router(work_orders.router, prefix=settings.API_V1_PREFIX)
app.include_router(gestion_produccion.router, prefix=settings.API_V1_PREFIX)
app.include_router(encargados.router, prefix=settings.API_V1_PREFIX)
app.include_router(logs.router, prefix=settings.API_V1_PREFIX)
app.include_router(auth.router, prefix=settings.API_V1_PREFIX)
app.include_router(counters.router, prefix=settings.API_V1_PREFIX)
app.include_router(dashboards.router, prefix=settings.API_V1_PREFIX)

@app.get("/health/db", tags=["health"])
async def health_db():
    db=get_db()
    pong= await db.command("ping")
    if pong.get("ok")==1.0: return {"ok": True,"db": db.name, 'message':'BD conectado correctamente.'}
    else: return {"ok": False,"db": db.name, 'message':'BD NO conectado!'}
