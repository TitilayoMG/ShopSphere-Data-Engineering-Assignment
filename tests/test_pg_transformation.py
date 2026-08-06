
import io
import logging
import pandas as pd
import pytest 
import pyarrow

from pipeline import transform

@pytest.fixture 
def sample_df():
    return pd.DataFrame({
        "product_id": [1, 2, 3],
        "brand": ["Apple", None, "Samsung"],
    })

def test_file_upload(fake_minio, sample_df, setup_transform):
    client, bucket = fake_minio("raw/postgres/products/products.parquet", sample_df)
    setup_transform(client, bucket)
   
    transform.transform_postgres()

    # NULL brand should become Unknown
    # Assert
    assert "processed/postgres/products/products.parquet" in client.uploaded

def test_null_to_unknown_values(fake_minio, sample_df, setup_transform):
    client, bucket = fake_minio("raw/postgres/products/products.parquet", sample_df)
    setup_transform(client, bucket)

    transform.transform_postgres()
    uploaded_bytes = client.uploaded[
        "processed/postgres/products/products.parquet"
    ]
    buffer = io.BytesIO(uploaded_bytes)
    result_df = pd.read_parquet(buffer)
    assert result_df['brand'][1] == 'Unknown'
    assert result_df['product_id'][1] == 2

def test_invalid_file_path(fake_minio, sample_df, setup_transform, caplog):
    caplog.set_level(logging.WARNING)
    
    client, bucket = fake_minio("raw/postgres/products.parquet", sample_df)
    setup_transform(client, bucket)

    transform.transform_postgres()

    assert "Skipping invalid path: raw/postgres/products.parquet" in caplog.text 
    assert client.uploaded == {}
    assert client.deleted == []

def test_file_remove(fake_minio, sample_df, setup_transform):
    client, bucket = fake_minio("raw/postgres/products/products.parquet", sample_df)
    setup_transform(client, bucket)
    transform.transform_postgres()
    assert len(client.deleted) == 0


def test_fail_postgres_transformation(fake_minio, setup_transform):
    client, bucket = fake_minio("raw/postgres/products/products.parquet", b"corrupted parquet")
    setup_transform(client, bucket)

    with pytest.raises(pyarrow.lib.ArrowInvalid):
        transform.transform_postgres()