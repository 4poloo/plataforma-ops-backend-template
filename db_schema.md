# Esquema de MongoDB (Plataforma SC)

Esta API usa MongoDB (DB definida por `MONGO_DB`) y varias colecciones. Los nombres de algunas colecciones se pueden parametrizar vía variables de entorno (`COLL_*`). A continuación, el esquema lógico, campos frecuentes e índices relevantes.

## Colecciones principales

### `products` (catálogo de productos/SKU)
- Campos típicos: `sku` (string), `nombre`, `c_barra` (int), `unidad`, `dg`/`dsg`, `codigo_g`/`codigo_sg`, `pneto`, `piva`, `tipo` (ej. PT/MP/INSUMO), `activo` (bool), `valor_repo`, `categoria` opcional, `audit.createdAt`/`audit.updatedAt`.
- Índices: `uniq_sku` (único por `sku`), `idx_categoria`, `idx_activo`, `idx_nombre`.
- Uso: CRUD y cargas masivas desde el front; es la base para recetas y OT.

### `recipes` (recetas de producto terminado)
- Estructura base:
  - `productPTId` (ObjectId de `products` tipo PT).
  - `vigenteVersion` (int opcional).
  - `versiones` (array):
    - `version` (int), `estado` (`borrador`|`vigente`|`obsoleta`), `fechaPublicacion` (date), `publicadoPor` (string).
    - `base_qty` (float), `unidad_PT` (string).
    - Proceso: `processId` (ObjectId a `processes`) o `procesoEspecial_nombre` + `procesoEspecial_costo`.
    - `componentes` (array de insumos): `productId` (ObjectId de MP/IN), `cantidadPorBase` (float), `unidad`, `merma_pct` (float).
  - `createdAt`/`updatedAt`.
- Índices: no se definen explícitos en código; agrega según tus queries (skuPT, vigenteVersion, etc.).

### `users`
- Campos: `email`, `email_ci` (lowercase), `alias`, `alias_ci` (lowercase), `passwordHash` (bcrypt), `nombre`, `apellido`, `role` (ej. `admin`, `operador`, etc.), `status` (`active`/`disabled`), `audit.createdAt`/`audit.updatedAt`.
- Índices: únicos en `email_ci` y `alias_ci`; `idx_status`, `idx_role`.
- Uso: login (`/auth/login`) valida `alias_ci` + `passwordHash`.

### `work_orders`
- Campos: `OT` (int, único), `contenido` (embed):
  - `SKU`, `Cantidad` (float), `Encargado`, `linea`, `fecha`, `fecha_ini`, `fecha_fin`, `descripcion` opcional.
- `estado` (`CREADA`|`EN PROCESO`|`CERRADA`), `merma` (float), `cantidad_fin` (float).
- `audit.createdAt`/`audit.updatedAt`.
- Índices: `uq_work_orders_ot` (único en `OT`).
- Uso: CRUD, importación masiva y envío a WMS (ver `services/wms_service.py`).

### `gestion_OT_prod`
- Replica la OT con más datos operativos:
  - `OT` (int, único), `contenido` con `SKU`, `Encargado`, `linea`, `fecha`, `fecha_ini`, `fecha_fin`, `hora_entrega` (time), `descripcion`, `cantidad_hora_extra`, `cantidad_hora_normal`.
  - `estado`, `merma`, `cantidad_fin`, `audit`.
- Índices: `uq_gestion_ot_prod_ot` (único en `OT`).
- Uso: cierres diarios (`tasks/daily_close.py`) y reportes de producción.

### `logs`
- Campos: `actor` (`admin`|`user`|`sistema`), `entity` (`recipe`, `user`, `encargado`, `work_order`, `product`), `event` (`create`, `update`, `modify`, `disable`, `delete`, `enable`), `userAlias`, `userAlias_ci`, `payload` (dict libre), `severity` (`INFO`|`WARN`), `loggedAt` (datetime), más metadata calculada (`accion`, `usuario`).
- Índices: `idx_loggedAt` (desc), `idx_severity`, `idx_userAlias_ci`.
- Uso: auditoría de acciones desde el front.

### `encargados`
- Campos: `nombre`, `linea`, `predeterminado` (bool), `audit` opcional.
- Índices: `idx_nombre_linea` (compuesto).
- Uso: catálogo de responsables por línea de producción.

### `counters`
- Campos: `_id` (string del contador, ej. `work_orders`), `seq` (int).
- Uso: correlativos (siguiente OT, etc.). Operaciones atómicas `update_seq`/`increment_seq`/`decrement_seq`.

## Colecciones parametrizables de integración S3 (DECLARE_PT)
- Nombres definidos por entorno: `COLL_DECLAREPT` (default `declare_pt_events`) y `COLL_CONSUMIRVASOT` (default `consume_vasot_events`).
- Datos ingestados desde S3 por `tasks/declarept_sync.py` + `utils/declarept_s3_sync.py`.
- Campos agregados:
  - `source_s3_key`, `ingested_at` (datetime UTC), `tipoEvento` (`DECLARE_PT` o `CONSUMIR_VASOT`), `stage` (env).
  - Payload del JSON original (libre) más claves comunes: `work_order`, `document_number`, `idlpn`, métricas de consumo/producción.
- Upsert key: `{ stage, work_order, document_number, idlpn }`.
- No hay índices declarados; se recomienda agregar índices compuestos sobre el filtro de upsert si consultas por esos campos.

## Otras colecciones mencionadas
- `processes`: catálogo de procesos productivos (usado en recetas). Campos esperados: `codigo`, costos, etc. (revisa tus datos).
- `import_batches`: temporal para cargas masivas de productos (creada desde `products_repo` si se usa import).

## Relaciones y convenciones
- Referencias por ObjectId:
  - `recipes.versiones.componentes[].productId` → `products._id`.
  - `recipes.productPTId` → `products._id` (tipo PT).
  - `recipes.versiones.processId` → `processes._id`.
  - OT y gestión usan `OT` como llave compartida (entero); no hay referencia directa a productos pero `contenido.SKU` debe existir en `products`.
- Campos `*_ci` guardan valores en minúsculas para búsquedas case-insensitive (`users`, `logs`).
- Auditoría: muchos documentos usan `audit.createdAt`/`audit.updatedAt` en UTC.

## Tareas y efectos en BD
- `lifespan` (`app/main.py`): conecta a Mongo y asegura índices en `products`, `work_orders`, `gestion_OT_prod`, `encargados`, `logs`.
- `tasks/daily_close.py`: marca `estado=CERRADA` en `gestion_OT_prod` y `work_orders` según fecha.
- `tasks/declarept_sync.py`: ingesta de JSON S3 a `COLL_DECLAREPT`/`COLL_CONSUMIRVASOT` con upsert y movimiento de archivos en S3.

## Recomendaciones de operación
- Antes de producción agrega índices adicionales según tus reportes (ej. `work_orders.estado`, `gestion_OT_prod.contenido.fecha`, `recipes.vigenteVersion`).
- Valida que los valores `COLL_*` de tu `.env` coincidan con las colecciones existentes en la DB.
- Mantén backups regulares de `declare_pt_events`/`consume_vasot_events` si son fuente única de trazabilidad.
