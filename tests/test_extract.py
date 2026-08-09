from datetime import datetime, timezone
from io import BytesIO
import pytest 
from unittest.mock import Mock 


from pipeline.postgres_pipeline import extract



def test_table_name():
    data_source, chunk_size = extract.load_config()

    assert isinstance(data_source, dict)
    assert isinstance(chunk_size, int)

    postgres_tables = [data['table'] for data in data_source['postgres']['datasets']]
    assert postgres_tables == ["customers","products","orders","order_items","payments"]

def test_cursor_execution(monkeypatch):
    fake_cursor = Mock()
    fake_cursor.description = [
        ("customer_id",),
        ("updated_at",),
    ]

    fake_cursor.fetchmany.side_effect = [
        [
            (1, datetime(2026, 8, 1, tzinfo=timezone.utc)),
            (2, datetime(2026, 8, 2, tzinfo=timezone.utc)),
        ],
        [],   # second call ends the loop
    ]
    fake_conn = Mock()
    fake_conn.cursor.return_value = fake_cursor

    fake_client = Mock(name='client')
    fake_read = Mock(return_value = ("2026-08-01", 1))

    monkeypatch.setattr(extract, "DATA_SOURCES", {"postgres": {"datasets": [{"table": "customers"}]}})
    monkeypatch.setattr(extract, "get_postgres_connection", lambda *_: fake_conn)
    monkeypatch.setattr(extract, "get_minio_client", lambda: (fake_client, "bucket"))
    monkeypatch.setattr(extract, "upload_to_minio", Mock())
    monkeypatch.setattr(extract, "records_to_parquet_buffer", lambda records: (None, BytesIO(b"abc")))
    monkeypatch.setattr(extract, "read_minio_watermark", fake_read)
    monkeypatch.setattr(extract, "write_minio_watermark", Mock())

    extract.postgres_extraction()
    # for call in fake_cursor.execute.call_args_list:
    #     print(call) #print will work if you run pytest -s tests/test_extract.py

    assert fake_conn.cursor.call_count == 2
    fake_cursor.execute.assert_any_call("SELECT * FROM customers LIMIT 0")
    # fake_cursor.execute.assert_any_call(
    #     """
    #     SELECT * 
    #     FROM customers
    #     WHERE updated_at > %s
    #     ORDER BY updated_at
    #     """,
    #      ("2026-08-01",)
    # ) # if theres an extra space between the letters it can result to assertion error
    sql, params = fake_cursor.execute.call_args_list[1].args

    assert "WHERE updated_at > %s" in sql
    assert "FROM customers" in sql
    assert params == ("2026-08-01",)

def test_read_write_watermark(monkeypatch):
    fake_cursor = Mock()
    fake_cursor.description = [
        ("customer_id",),
        ("updated_at",),
    ]

    fake_cursor.fetchmany.side_effect = [
        [
            (1, datetime(2026, 8, 1, tzinfo=timezone.utc)),
            (2, datetime(2026, 8, 2, tzinfo=timezone.utc)),
        ],
        [],   # second call ends the loop
    ]
    fake_conn = Mock()
    fake_conn.cursor.return_value = fake_cursor

    fake_client = Mock(name='client')
    fake_read = Mock(return_value = ("2026-08-01", 1))
    fake_write = Mock()
    fake_upload = Mock()

    monkeypatch.setattr(extract, "DATA_SOURCES", {"postgres": {"datasets": [{"table": "customers"}]}})
    monkeypatch.setattr(extract, "get_postgres_connection", lambda *_: fake_conn)
    monkeypatch.setattr(extract, "get_minio_client", lambda: (fake_client, "bucket"))
    monkeypatch.setattr(extract, "upload_to_minio", fake_upload)
    monkeypatch.setattr(extract, "records_to_parquet_buffer", lambda records: (None, BytesIO(b"abc")))
    monkeypatch.setattr(extract, "read_minio_watermark", fake_read)
    monkeypatch.setattr(extract, "write_minio_watermark", fake_write)

    extract.postgres_extraction()

    fake_read.assert_called_once()
    fake_write.assert_called_once()