import io 
import pandas as pd
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_integer_dtype,
)
import pytest 
from pipeline import transform 


@pytest.fixture 
def shipment_sample_data():
    return pd.DataFrame(
        [
            {
                "shipment_id": "SHP001",
                "order_id": 1001,
                "carrier_id": "CAR001",
                "tracking_number": "TRK123456",
                "status": "Delivered",
                "shipped_at": "2026-07-01 09:00:00",
                "estimated_delivery_at": "2026-07-03 18:00:00",
                "delivered_at": "2026-07-03 16:45:00",
                "updated_at": "2026-07-03 16:50:00",
                "delivery_address": "12 Allen Avenue",
                "delivery_address_extra": "Apartment 3B",
                "events": [
                    {
                        "event_time": "2026-07-01 09:05:00",
                        "event_type": "Picked Up",
                        "location": "Lagos",
                    },
                    {
                        "event_time": "2026-07-02 14:20:00",
                        "event_type": "In Transit",
                        "location": "Ibadan",
                    },
                    {
                        "event_time": "2026-07-03 16:45:00",
                        "event_type": "Delivered",
                        "location": "Abuja",
                    },
                ],
            },
            {
                "shipment_id": "SHP002",
                "order_id": 1002,
                "carrier_id": "CAR002",
                "tracking_number": "TRK654321",
                "status": "In Transit",
                "shipped_at": "2026-07-04 10:15:00",
                "estimated_delivery_at": "2026-07-06 18:00:00",
                "delivered_at": None,
                "updated_at": "2026-07-05 08:30:00",
                "delivery_address": "45 Admiralty Way",
                "delivery_address_extra": None,
                "events": [
                    {
                        "event_time": "2026-07-04 10:30:00",
                        "event_type": "Picked Up",
                        "location": "Port Harcourt",
                    },
                    {
                        "event_time": "invalid-date",
                        "event_type": "Sorting Facility",
                        "location": "Benin",
                    },
                ],
            },
            # Duplicate record (should be removed by drop_duplicates)
            {
                "shipment_id": "SHP002",
                "order_id": 1002,
                "carrier_id": "CAR002",
                "tracking_number": "TRK654321",
                "status": "In Transit",
                "shipped_at": "2026-07-04 10:15:00",
                "estimated_delivery_at": "2026-07-06 18:00:00",
                "delivered_at": None,
                "updated_at": "2026-07-05 08:30:00",
                "delivery_address": "45 Admiralty Way",
                "delivery_address_extra": None,
                "events": [
                    {
                        "event_time": "2026-07-04 10:30:00",
                        "event_type": "Picked Up",
                        "location": "Port Harcourt",
                    },
                    {
                        "event_time": "invalid-date",
                        "event_type": "Sorting Facility",
                        "location": "Benin",
                    },
                ],
            },
        ]
    )

def test_duplicate_rows_in_shipments_data(fake_minio, shipment_sample_data, setup_transform):
    client, bucket = fake_minio("raw/api/shipments/shipments.parquet", shipment_sample_data) 

    setup_transform(client, bucket)
    transform.transform_api()

    uploaded_df = client.uploaded["processed/api/shipments/shipments.parquet"]
    buffer = io.BytesIO(uploaded_df)
    df = pd.read_parquet(buffer)
    assert df.duplicated().sum() == 0

def test_data_types_in_shipments_data(fake_minio, shipment_sample_data, setup_transform):
    client, bucket = fake_minio("raw/api/shipments/shipments.parquet", shipment_sample_data) 

    setup_transform(client, bucket)
    transform.transform_api()

    uploaded_df = client.uploaded["processed/api/shipments/shipments.parquet"]
    buffer = io.BytesIO(uploaded_df)
    df = pd.read_parquet(buffer)
    assert is_datetime64_any_dtype(df["shipped_at"]) # or assert str(df["created_at"].dtype).startswith("datetime64")
    assert str(df["delivered_at"].dtype).startswith("datetime64")
    assert is_integer_dtype(df["order_id"])

def test_dropped_columns_in_shpmt_data(fake_minio, shipment_sample_data, setup_transform):
    client, bucket = fake_minio("raw/api/shipments/shipments.parquet", shipment_sample_data) 

    setup_transform(client, bucket)
    transform.transform_api()

    uploaded_df = client.uploaded["processed/api/shipments/shipments.parquet"]
    buffer = io.BytesIO(uploaded_df)
    df = pd.read_parquet(buffer)  
    assert "events" not in df.columns
    assert "delivery_address_extra" not in df.columns
    assert "order_id" in df.columns

def test_carriers_transformations(fake_minio, setup_transform):
    df = pd.DataFrame(
        [
            {
                "carrier_id": "CAR001",
                "carrier_name": " DHL ",
                "contact_email": " support@dhl.com ",
                "phone": " +2348012345678 ",
            },
            {
                "carrier_id": "CAR002",
                "carrier_name": " FedEx ",
                "contact_email": " contact@fedex.com ",
                "phone": " +2348098765432 ",
            },
            # Duplicate row (tests drop_duplicates)
            {
                "carrier_id": "CAR002",
                "carrier_name": " FedEx ",
                "contact_email": " contact@fedex.com ",
                "phone": " +2348098765432 ",
            },
        ]   
    )
    client, bucket = fake_minio("raw/api/carriers/carriers.parquet", df)
    setup_transform(client, bucket)

    transform.transform_api()
    uploaded_data = client.uploaded["processed/api/carriers/carriers.parquet"]
    buffer = io.BytesIO(uploaded_data)
    df = pd.read_parquet(buffer)

    assert df.duplicated().sum() == 0
    # checking .str.strip() of all rows 
    assert df["carrier_id"].eq(df["carrier_id"].str.strip()).all() # or assert df["carrier_id"].equals(df["carrier_id"].str.strip())
    assert df["carrier_name"].eq(df["carrier_name"].str.strip()).all()
    assert df['phone'].eq(df['phone'].str.strip()).all()
