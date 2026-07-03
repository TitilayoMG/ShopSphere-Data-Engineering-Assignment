from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query

from models import Carrier, Shipment

DATA_DIR = Path(__file__).resolve().parent / "data"
MAX_PAGE_SIZE = 100

app = FastAPI(
    title="SwiftDrop Logistics Mock API",
    description="Local third-party logistics API for the ShopSphere data engineering case study.",
    version="1.0.0",
)


def _load_json(filename: str) -> list[dict[str, Any]]:
    with (DATA_DIR / filename).open("r", encoding="utf-8") as file:
        return json.load(file)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


CARRIERS = _load_json("carriers.json")
SHIPMENTS = _load_json("shipments.json")
SHIPMENT_BY_ID = {shipment["shipment_id"]: shipment for shipment in SHIPMENTS}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "healthy"}


@app.get("/api/v1/carriers", response_model=list[Carrier])
def list_carriers() -> list[dict[str, Any]]:
    return CARRIERS


@app.get("/api/v1/shipments")
def list_shipments(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=MAX_PAGE_SIZE),
    status: str | None = Query(None),
    updated_since: str | None = Query(None),
) -> dict[str, Any]:
    filtered = SHIPMENTS
    if status:
        filtered = [item for item in filtered if item.get("shipment_status") == status]
    if updated_since:
        cutoff = _parse_datetime(updated_since)
        if cutoff is None:
            raise HTTPException(status_code=400, detail="updated_since must be an ISO-8601 datetime")
        filtered = [item for item in filtered if (_parse_datetime(item.get("updated_at")) or datetime.min.replace(tzinfo=timezone.utc)) > cutoff]

    total = len(filtered)
    total_pages = math.ceil(total / limit) if total else 0
    start = (page - 1) * limit
    end = start + limit
    return {
        "page": page,
        "limit": limit,
        "total": total,
        "total_pages": total_pages,
        "next_page": page + 1 if page < total_pages else None,
        "shipments": filtered[start:end],
    }


@app.get("/api/v1/shipments/{shipment_id}", response_model=Shipment)
def get_shipment(shipment_id: str) -> dict[str, Any]:
    shipment = SHIPMENT_BY_ID.get(shipment_id)
    if shipment is None:
        raise HTTPException(status_code=404, detail="Shipment not found")
    return shipment
