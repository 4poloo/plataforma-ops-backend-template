from __future__ import annotations

import csv
import io

from typing import Literal

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse

from app.db.mongo import get_db
from app.models.work_orders import (
    WorkOrderCreateIn,
    WorkOrderOut,
    WorkOrderIntegrationRequest,
    WorkOrderIntegrationResponse,
    WorkOrderListFilters,
    WorkOrderStatusOut,
    WorkOrderEstadoUpdate,
    RecipePrintRequest,
    NextWorkOrderOut,
    LastWorkOrderOut,
    WorkOrderBulkImportOut,
)
from app.services import WO, wms_service

router = APIRouter(prefix="/work-orders", tags=["work-orders"])


@router.post("", response_model=WorkOrderOut)
async def create_work_order(body: WorkOrderCreateIn, db=Depends(get_db)):
    try:
        return await WO.create_work_order(db, body)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@router.get("/next", response_model=NextWorkOrderOut)
async def get_next_work_order_number(db=Depends(get_db)):
    next_ot = await WO.get_next_ot(db)
    return {"next": next_ot}


@router.get("/last", response_model=LastWorkOrderOut)
async def get_last_work_order_number(db=Depends(get_db)):
    try:
        last_ot = await WO.get_last_created_ot(db)
    except ValueError as exc:
        message = str(exc)
        status = 404 if "no hay" in message.lower() else 422
        raise HTTPException(status_code=status, detail=message)
    return {"OT": last_ot}


@router.get("/template", response_class=StreamingResponse)
async def download_template():
    header = ["OT", "SKU", "Cantidad", "Encargado", "linea", "fecha", "fecha_ini", "fecha_fin"]
    sample_row = {
        "OT": "1001",
        "SKU": "PT-0001",
        "Cantidad": "100",
        "Encargado": "Juan Perez",
        "linea": "Linea 1",
        "fecha": "2024-01-15",
        "fecha_ini": "2024-01-20",
        "fecha_fin": "2024-01-25",
    }

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=header)
    writer.writeheader()
    writer.writerow(sample_row)
    buffer.seek(0)

    response = StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
    )
    response.headers["Content-Disposition"] = "attachment; filename=work_orders_template.csv"
    return response


@router.post("/import", response_model=WorkOrderBulkImportOut)
async def import_work_orders(file: UploadFile = File(...), db=Depends(get_db)):
    try:
        content = (await file.read()).decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="No se pudo decodificar el CSV (utf-8)")

    try:
        return await WO.import_work_orders_from_csv(db, content)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/recipe/print")
async def download_recipe_pdf(
    skuPT: str = Query(..., min_length=1),
    cantidad: float | None = Query(None, ge=0),
    numeroOT: str | None = Query(None),
    encargado: str | None = Query(None),
    fecha_ini: date | None = Query(None),
    db=Depends(get_db),
):
    payload = RecipePrintRequest(
        skuPT=skuPT,
        cantidad=cantidad,
        numeroOT=numeroOT,
        encargado=encargado,
        fecha_ini=fecha_ini,
    )
    try:
        filename, file_stream = await WO.generate_recipe_pdf(db, payload)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="No se encontró la plantilla de receta")
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc) or "Error al generar la receta en PDF")

    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        file_stream,
        media_type="application/pdf",
        headers=headers,
    )


@router.get("/{ot}/print")
async def download_work_order_pdf(ot: int, db=Depends(get_db)):
    try:
        filename, file_stream = await WO.generate_work_order_pdf(db, ot)
    except ValueError as exc:
        message = str(exc)
        status = 404 if "no encontrada" in message.lower() else 422
        raise HTTPException(status_code=status, detail=message)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        file_stream,
        media_type="application/pdf",
        headers=headers,
    )


@router.get("/{ot}", response_model=WorkOrderOut)
async def get_work_order(ot: int, db=Depends(get_db)):
    try:
        return await WO.get_work_order_by_ot(db, ot)
    except ValueError as exc:
        message = str(exc)
        status = 404 if "no encontrada" in message.lower() else 422
        raise HTTPException(status_code=status, detail=message)


@router.get("", response_model=list[WorkOrderOut])
async def list_work_orders(
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    estado: Literal["CREADA", "EN PROCESO", "CERRADA"] | None = Query(
        None,
        description="Filtra por estado de la OT.",
    ),
    db=Depends(get_db),
):
    filters = WorkOrderListFilters(estado=estado) if estado else None
    return await WO.list_work_orders(db, limit=limit, skip=skip, filters=filters)


@router.post("/integration/send", response_model=WorkOrderIntegrationResponse)
async def send_work_orders_to_wms(body: WorkOrderIntegrationRequest, db=Depends(get_db)):
    try:
        payload_items = body.payload
        payload_built_from_ot = False
        if payload_items is None:
            if body.OT is None or body.contenido is None:
                raise ValueError("Se requiere 'payload' o los datos de la OT (OT + contenido).")
            payload_items = await WO.build_wms_integration_items(
                db,
                ot=int(body.OT),
                contenido=body.contenido,
            )
            payload_built_from_ot = True
        if not payload_built_from_ot:
            payload_items = await WO.filter_wms_payload_items(db, payload_items)

        request_to_send = WorkOrderIntegrationRequest(
            source=body.source or "portal",
            payload=payload_items,
        )
        return await wms_service.send_work_orders(request_to_send)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except wms_service.WMSIntegrationError as exc:
        raise HTTPException(
            status_code=502,
            detail={"message": str(exc), "status_code": exc.status_code, "body": exc.body},
        )


@router.post("/{code}/status", response_model=WorkOrderStatusOut)
async def get_work_order_status(
    code: str,
    env: Literal["qa", "prod"] | None = None,
):
    try:
        return await wms_service.query_work_order_status(code, target_env=env)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except wms_service.WMSIntegrationError as exc:
        raise HTTPException(
            status_code=502,
            detail={"message": str(exc), "status_code": exc.status_code, "body": exc.body},
        )


@router.patch("/{ot}/estado", response_model=WorkOrderOut)
async def update_work_order_estado(
    ot: int,
    body: WorkOrderEstadoUpdate,
    db=Depends(get_db),
):
    try:
        return await WO.update_work_order_estado(db, ot, body)
    except ValueError as exc:
        message = str(exc)
        status = 404 if "no encontrada" in message.lower() else 422
        raise HTTPException(status_code=status, detail=message)
