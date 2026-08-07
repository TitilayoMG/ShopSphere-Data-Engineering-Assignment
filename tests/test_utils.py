from io import BytesIO

import pytest 
from unittest.mock import Mock
from pipeline.postgres_pipeline import utils



def test_get_minio_client(monkeypatch):
    # Create a fake Minio constructor
    fake_minio = Mock(name="Minio")
    monkeypatch.setattr(utils, "Minio", fake_minio)

    # Fake environment variables
    monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "minio")
    monkeypatch.setenv("MINIO_SECRET_KEY", "password")
    monkeypatch.setenv("MINIO_SECURE", "false")
    monkeypatch.setenv("MINIO_BUCKET", "test-bucket")

    client, bucket = utils.get_minio_client()

    # Minio() was called correctly
    fake_minio.assert_called_once_with(
        endpoint="localhost:9000",
        access_key="minio",
        secret_key="password",
        secure=False,
    )

    # Returned values
    assert client == fake_minio.return_value
    assert bucket == "test-bucket"

def test_upload_to_minio():
    client = Mock()
    buffer = BytesIO(b"hello")

    utils.upload_to_minio(
        client,
        "test-bucket",
        "folder/file.txt",
        buffer,
        "text/plain",
    )

    client.put_object.assert_called_once_with(
        bucket_name="test-bucket",
        object_name="folder/file.txt",
        data=buffer,
        length=5,          # len(b"hello")
        content_type="text/plain",
    )

def test_postgres_connection(monkeypatch):
    fake_conn = Mock()
    monkeypatch.setattr(utils.psycopg2, "connect", fake_conn)

    monkeypatch.setenv("WAREHOUSE_POSTGRES_HOST", "localhost:5432")
    monkeypatch.setenv("WAREHOUSE_POSTGRES_PORT", "5432")
    monkeypatch.setenv("WAREHOUSE_POSTGRES_DB", "database")
    monkeypatch.setenv("WAREHOUSE_POSTGRES_USER", "postgres")
    monkeypatch.setenv("WAREHOUSE_POSTGRES_PASSWORD", "password")

    conn = utils.get_postgres_connection()

    fake_conn.assert_called_once_with(
        host="localhost:5432",
        port = "5432",
        database="database",
        user = "postgres",
        password ="password"
    )

    assert conn == fake_conn.return_value