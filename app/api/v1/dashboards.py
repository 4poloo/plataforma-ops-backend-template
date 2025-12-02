from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Path

from app.db.mongo import get_db
from app.models.dashboards import DashboardNetSkusOut
from app.services import dashboards as dashboards_service

router = APIRouter(prefix="/dashboards", tags=["dashboards"])


@router.get("/ot/{ot}/skus", response_model=DashboardNetSkusOut)
async def get_dashboard_net_skus(
    ot: int = Path(..., ge=1, description="Número de OT sin prefijo"),
    db=Depends(get_db),
):
    try:
        net = await dashboards_service.get_net_skus_by_ot(db, ot)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    return {"OT": int(ot), "work_order": f"OT-{int(ot)}", "skus": net}
