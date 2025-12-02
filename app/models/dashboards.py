from __future__ import annotations

from typing import Dict

from pydantic import BaseModel, Field


class DashboardNetSkusOut(BaseModel):
    OT: int = Field(..., ge=1)
    work_order: str
    skus: Dict[str, float] = Field(
        ...,
        description="Netos por SKU: DECLARE_PT menos CONSUMIR_VASOT",
    )

