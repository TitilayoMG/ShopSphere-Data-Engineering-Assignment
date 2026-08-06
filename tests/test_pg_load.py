
import pandas as pd 
from unittest.mock import Mock
from pipeline import load

def test_postgres_load_duplicates(monkeypatch, fake_minio):
    df = pd.DataFrame({
        "product_id": [1, 2, 3],
        "brand": ["Apple", None, "Samsung"],
    })
    client, bucket = fake_minio("processed/postgres/products/products.parquet", df)
    conn = Mock()
    cursor = conn.cursor.return_value
    # monkeypatch.setattr(load, "LOAD_ORDER", ["products"])
    monkeypatch.setattr(load, "get_postgres_connection", lambda *_: conn)
    monkeypatch.setattr(load, "mark_pipeline_fail", Mock())
    monkeypatch.setattr(load, "mark_pipeline_success", Mock())
    monkeypatch.setattr(load, "read_pipeline_runs", Mock(return_value=False))
    monkeypatch.setattr(load, "start_pipeline_run", Mock(return_value=1))
    monkeypatch.setattr(load, "update_pipeline_watermark", Mock())
    monkeypatch.setattr(load, "get_minio_client", lambda: (client, bucket))

    load.load_all_data_source_files()

    # this means: Give me the first argument that was passed to copy_expert()
    copy_sql = cursor.copy_expert.call_args.args[0]
    assert "COPY public.stg_products" in copy_sql

    # bcos cursor.execute() was called more than once 
    # so it returns the first argument passed to the first call 
    insert_sql = cursor.execute.call_args_list[0].args[0]
    assert "INSERT INTO public.products" in insert_sql
    assert "ON CONFLICT (product_id) DO NOTHING" in insert_sql

    # returns the second arg passed to cursor.execute()
    truncate_sql = cursor.execute.call_args_list[1].args[0]
    assert "TRUNCATE TABLE public.stg_products" in truncate_sql

    load.update_pipeline_watermark.assert_called_once()