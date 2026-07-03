from __future__ import annotations

from typing import Any
from pydantic import BaseModel, Field


class Address(BaseModel):
    """Delivery address returned by SwiftDrop Logistics."""

    street: str | None = None
    city: str | None = None
    state: str | None = None
    country: str | None = None
    postal_code: str | None = None


class ShipmentEvent(BaseModel):
    """Single logistics status-change event."""

    event_type: str
    event_time: str
    location: str | None = None
    notes: str | None = None
    metadata: dict[str, Any] | None = None


class Shipment(BaseModel):
    """Shipment resource exposed by the mock logistics API."""

    shipment_id: str
    order_id: int
    carrier_id: str
    tracking_number: str
    shipment_status: str
    shipped_at: str | None = None
    estimated_delivery_at: str | None = None
    delivered_at: str | None = None
    updated_at: str
    delivery_address: Address | dict[str, Any]
    events: list[ShipmentEvent | dict[str, Any]] = Field(default_factory=list)


class Carrier(BaseModel):
    """Carrier profile returned by /api/v1/carriers."""

    carrier_id: str
    carrier_name: str
    service_level: str
    support_phone: str
