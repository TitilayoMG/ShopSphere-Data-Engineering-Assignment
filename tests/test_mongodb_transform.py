
import io
import logging
import pandas as pd
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_integer_dtype,
    is_bool_dtype,
)

import pytest
from unittest.mock import Mock
from pipeline.postgres_pipeline import transform


@pytest.fixture 
def fake_df():
    df = pd.DataFrame(
        [
            {
                "session_id": "S001",
                "customer_id": 1,
                "started_at": "2026-07-01 10:00:00",
                "ended_at": "2026-07-01 10:30:00",
                "device": {
                    "type": "Mobile",
                    "os": "Android",
                },
                "events": [
                    {
                        "event_time": "2026-07-01 10:05:00",
                        "event_type": "view",
                        "product_id": 101,
                        "quantity": 1,
                    },
                    {
                        "event_time": "2026-07-01 10:10:00",
                        "event_type": "add_to_cart",
                        "product_id": 101,
                        "quantity": 1,
                    },
                ],
            },
            {
                "session_id": "S002",
                "customer_id": 2,
                "started_at": "2026-07-02 09:00:00",
                "ended_at": "2026-07-02 09:20:00",
                "device": {
                    "type": "Desktop",
                    "os": "Windows",
                },
                "events": [
                    {
                        "event_time": "2026-07-02 09:05:00",
                        "event_type": "purchase",
                        "product_id": 202,
                        "quantity": 2,
                    },
                    {
                        # Duplicate event to test drop_duplicates()
                        "event_time": "2026-07-02 09:05:00",
                        "event_type": "purchase",
                        "product_id": 202,
                        "quantity": 2,
                    },
                ],
            },
        ]
    )
    return df 

# mongodb: product review
def test_product_reviews_transformation(monkeypatch, fake_minio, fake_upload):
    df = pd.DataFrame(
        {
            "customer_id": [1, 1, 2],
            "product_id": [100, 100, 200],
            "rating": [5, 5, 4],
            "helpful_votes": [3, 3, 2],

            "verified_purchase": [
                False,
                None,
                True
            ],

            "review_text": [
                None,
                " Good product",
                "Average"
            ],

            "title": [
                " Nice",
                "Nice",
                None
            ],

            "created_at": [
                "2026-01-01",
                "2026-01-01",
                "invalid-date"
            ]
        }
    )
    client, bucket = fake_minio("raw/mongodb/product_reviews/products_test.parquet", df)
    monkeypatch.setattr(transform, "get_minio_client",  lambda *_: (client, bucket))
    monkeypatch.setattr(transform, "upload_to_minio", fake_upload)
    monkeypatch.setattr(transform, "DataQualityValidator", Mock())
    
    transform.transform_mongodb()

    output_path = client.uploaded["processed/mongodb/product_reviews/products_test.parquet"]

    output = io.BytesIO(output_path)
    df = pd.read_parquet(output)

    assert is_datetime64_any_dtype(df["created_at"]) # or assert str(df["created_at"].dtype).startswith("datetime64")
    assert is_integer_dtype(df["customer_id"])
    assert is_integer_dtype(df["product_id"])
    assert is_integer_dtype(df["rating"])
    assert is_integer_dtype(df["helpful_votes"])

    # Boolean column
    assert is_bool_dtype(df["verified_purchase"])

    # Nulls replaced
    assert df["verified_purchase"].isna().sum() == 0
    assert df["review_text"].isna().sum() == 0
    assert df["title"].isna().sum() == 0

    # Strings stripped
    assert all(df["review_text"] == df["review_text"].str.strip())
    assert all(df["title"] == df["title"].str.strip())

    # Duplicates removed
    assert not df.duplicated().any() # or assert df.duplicated().sum() == 0

    assert (df["review_text"][0] == "")
    assert (df["title"][2] == "")
    assert (df["verified_purchase"][1] == False)

# mongodb: customer sessions
def test_customer_sessions_transformation( fake_df, fake_minio, fake_upload,monkeypatch):
    client, bucket = fake_minio('raw/mongodb/customer_sessions/customer_sessions.parquet', fake_df)

    monkeypatch.setattr(transform, "get_minio_client",  lambda *_: (client, bucket))
    monkeypatch.setattr(transform, "upload_to_minio", fake_upload)
    monkeypatch.setattr(transform, "DataQualityValidator", Mock())
    
    transform.transform_mongodb() 

    uploaded_file = client.uploaded["processed/mongodb/customer_sessions/customer_sessions.parquet"]
    buffer = io.BytesIO(uploaded_file)
    df = pd.read_parquet(buffer)
    assert not df.duplicated().any() # or assert df.duplicated().sum() == 0
    assert "events" not in df.columns
    assert "device" not in df.columns
    assert "event_time" in df.columns
    assert "event_type" in df.columns
   
def test_file_upload( fake_df,fake_minio, fake_upload, monkeypatch):
    client, bucket = fake_minio(
        "raw/mongodb/customer_sessions/customer_sessions.parquet",
        fake_df
    )
    monkeypatch.setattr(transform, "get_minio_client",  lambda *_: (client, bucket))
    monkeypatch.setattr(transform, "upload_to_minio", fake_upload)
    monkeypatch.setattr(transform, "DataQualityValidator", Mock())
    
    transform.transform_mongodb()
    assert "processed/mongodb/customer_sessions/customer_sessions.parquet" in client.uploaded

def test_invalid_file_path(fake_minio, fake_upload, monkeypatch, caplog):
    caplog.set_level(logging.WARNING)
    df = pd.DataFrame({
        "product_id": [1, 2, 3],
        "brand": ["Apple", None, "Samsung"],
    })
    client, bucket = fake_minio(
            "raw/mongodb/products.parquet",
            df,
        )
    monkeypatch.setattr(transform, "get_minio_client",  lambda *_: (client, bucket))
    monkeypatch.setattr(transform, "upload_to_minio", fake_upload)
    monkeypatch.setattr(transform, "DataQualityValidator", Mock())
    
    transform.transform_mongodb()
    assert "Skipping invalid object raw/mongodb/products.parquet" in caplog.text

def test_unknown_collection(fake_minio, fake_df, fake_upload, monkeypatch, caplog):
    caplog.set_level(logging.WARNING)
    client, bucket = fake_minio('raw/mongodb/customers/customer_sessions.parquet', fake_df)

    monkeypatch.setattr(transform, "get_minio_client",  lambda *_: (client, bucket))
    monkeypatch.setattr(transform, "upload_to_minio", fake_upload)
    monkeypatch.setattr(transform, "DataQualityValidator", Mock())
    

    transform.transform_mongodb()
    assert "Unknown collection" in caplog.text # or Unknown collection 'customers' or 'customers'