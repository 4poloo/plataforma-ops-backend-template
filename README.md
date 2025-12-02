# Plataforma SC – Backend

Backend FastAPI + MongoDB para la plataforma de operaciones de SurChile. Expone APIs para autenticación, usuarios, productos, recetas, órdenes de trabajo, dashboards y sincronizaciones con WMS y S3. Incluye tareas en background para cierres diarios y para importar eventos desde S3.

## Tabla de contenido
- Requisitos previos
- Variables de entorno
- Puesta en marcha
- Estructura del proyecto
- Endpoints y módulos principales
- Sincronizaciones y tareas en background
- Utilidades y plantillas
- Notas de seguridad

## Requisitos previos
- Python 3.11+ (usa `venv`)
- MongoDB accesible según tu `MONGO_URI`/`MONGO_DB`
- Credenciales de AWS para lectura/escritura de S3 (DECLAREPT/CONSUMIR_VASOT)
- (Opcional) Acceso a los endpoints WMS configurados en las URLs de entorno

## Variables de entorno
No subas tu `.env` al repo. Copia el ejemplo y completa los secretos:

```bash
cp .env.example .env
```

Variables relevantes:
- `MONGO_URI`, `MONGO_DB`: conexión a MongoDB.
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_S3_BUCKET`, `AWS_S3_PREFIX_DECLAREPT`: acceso a S3 para sincronizar eventos de plataforma.
- `WMS_URL`, `WMS_USER`, `WMS_PASS`, `WMS_QUERY_URL_*`, `WMS_LOGIN_URL_*`: integración de órdenes de trabajo con WMS.
- `DECLAREPT_SYNC_INTERVAL_SECONDS`: intervalo para la tarea que lee S3 (mínimo 60s).
- Colecciones: `COLL_PRODUCTOS`, `COLL_RECETAS`, `COLL_RECETAS_STAGING`, `COLL_DECLAREPT`, `COLL_CONSUMIRVASOT`.

## Puesta en marcha
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configura tu .env antes de levantar la API
uvicorn app.main:app --reload --port 8000
```
- Swagger UI: `http://localhost:8000/docs`
- OpenAPI JSON: `http://localhost:8000/openapi.json`

## Estructura del proyecto
```
app/
├─ main.py                # Arranque FastAPI, CORS, routers, lifespan (conexión Mongo, índices, tareas)
├─ core/                  # Configuración, logging, seguridad (hash/verify bcrypt)
├─ api/v1/                # Routers HTTP (auth, users, products, recipes, work_orders, dashboards, etc.)
├─ db/                    # Cliente Mongo y repositorios (queries, índices)
├─ services/              # Lógica de negocio (auth, WMS, recipes, OT, dashboards)
├─ clients/               # Clientes HTTP externos (WMS/Invas/Altanet)
├─ tasks/                 # Tareas en background (cierre diario, sync S3)
├─ utils/                 # Helpers (validaciones, tiempo, sync S3 declarpt)
├─ domain/                # Catálogos/constantes (familias_map)
└─ plantillas/            # Plantillas Excel para cargas masivas
```

## Endpoints y módulos principales (API v1)
- `GET /health/db`: ping a Mongo.
- Auth: `POST /auth/login` (alias + password; valida hash bcrypt).
- Usuarios: CRUD en `app/api/v1/users.py`.
- Productos: CRUD, búsqueda por SKU/nombre, carga masiva CSV/Excel con validaciones (`app/api/v1/products.py` + índices únicos por SKU).
- Recetas: creación/edición, valoración, descarga de receta imprimible (`app/api/v1/recipes.py`, `services/recipes_valuation.py`).
- Órdenes de trabajo: creación/actualización/estado, importación en lote, integración con WMS (`app/api/v1/work_orders.py`, `services/wms_service.py`).
- Gestión producción (conteos/estadísticas de OT) en `app/api/v1/gestion_produccion.py`.
- Encargados: catálogo de responsables (`app/api/v1/encargados.py`).
- Dashboards: agregados y KPIs para frontend (`app/api/v1/dashboards.py`).
- Counters: siguiente número de OT, último correlativo (`app/api/v1/counters.py`).
- Logs: trazas de eventos de la plataforma (`app/api/v1/logs.py`).

## Sincronizaciones y tareas en background
- Lifespan en `app/main.py`:
  - Conecta a Mongo, asegura índices para products/work_orders/gestion_ot_prod/encargados/logs.
  - Lanza tareas en background.
- `tasks/daily_close.py`: a medianoche cierra OT pendientes del día anterior (`services/gestion_ot_prod.close_previous_day_entries`).
- `tasks/declarept_sync.py` + `utils/declarept_s3_sync.py`: cada `DECLAREPT_SYNC_INTERVAL_SECONDS` lee JSON desde S3 (`AWS_S3_BUCKET` + prefijos), evita duplicados por `idlpn`, inserta en `COLL_DECLAREPT` y `COLL_CONSUMIRVASOT`, y mueve archivos a `PROCECCED/` o `PROCECCED/ERRORS/`.
- Integración WMS (`services/wms_service.py`):
  - Envía órdenes (`send_work_orders`) con Basic Auth o token.
  - Consulta estado (`fetch_work_order_status`) con login previo y cache de token.
  - URLs de QA/Prod seleccionadas según `APP_ENV`.

## Utilidades y plantillas
- Plantillas Excel en `app/plantillas/` para cargas masivas de recetas y OT.
- Helpers de validación de fechas/horas (`utils/time.py`, `utils/validation.py`).
- Mapas de familia/códigos en `domain/familias_map.py` usados al importar productos.

## Notas de seguridad
- No subas el `.env`; usa `.env.example` como referencia.
- Las credenciales de AWS/WMS/Mongo se deben cargar como variables de entorno en cada entorno (local, QA, prod).
- Revisa `ALLOWED_ORIGINS` en `app/main.py` antes de exponer a internet. En producción usa una lista específica de dominios.
