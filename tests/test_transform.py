import io
import pandas as pd
import pipeline.transform as transform


def test_null_brand_transformation(fake_minio, monkeypatch):
    
    # replace global client and bucket created during import
    monkeypatch.setattr(
        transform,
        "get_minio_client",
        lambda: (fake_minio, "test-bucket")
    )

    # Mock upload_to_minio
    def fake_upload(
        client,
        bucket,
        object_name,
        buffer,
        content_type
    ):
        buffer.seek(0)

        client.uploaded[object_name] = buffer.read()
    
    monkeypatch.setattr(
        transform,
        "upload_to_minio",
        fake_upload
    )


    transform.transform_postgres()


    output_path = (
        "processed/postgres/products/products_test.parquet"
    )

    assert output_path in fake_minio.uploaded


    # Read transformed parquet
    output = io.BytesIO(
        fake_minio.uploaded[output_path]
    )

    df = pd.read_parquet(output)


    # NULL brand should become Unknown
    assert (
        df.loc[
            df["product_name"] == "Laptop",
            "brand"
        ].iloc[0]
        == "Unknown"
    )


    # Existing brand should remain unchanged
    assert (
        df.loc[
            df["product_name"] == "Phone",
            "brand"
        ].iloc[0]
        == "Apple"
    )


def test_product_reviews_transformation(
    fake_mongodb_minio,
    monkeypatch
):

    monkeypatch.setattr(
        transform,
        "get_minio_client",
        lambda: (
            fake_mongodb_minio,
            "test-bucket"
        )
    )


    def fake_upload(
        client,
        bucket,
        object_name,
        buffer,
        content_type
    ):
        buffer.seek(0)

        client.uploaded[object_name] = (
            buffer.read()
        )


    monkeypatch.setattr(
        transform,
        "upload_to_minio",
        fake_upload
    )


    transform.transform_mongodb()


    output_path = (
        "processed/mongodb/product_reviews/reviews_test.parquet"
    )


    assert (
        output_path
        in fake_mongodb_minio.uploaded
    )


    output = io.BytesIO(
        fake_mongodb_minio.uploaded[output_path]
    )


    df = pd.read_parquet(output)


    # assert (
    #     df["review_text"].iloc[0]
    #     == ""
    # )


    # assert (
    #     df["title"].iloc[0]
    #     == ""
    # )


    assert (
        df["verified_purchase"].iloc[0]
        == False
    )

     # -------------------------------
    # Test datetime conversion
    # -------------------------------

    assert pd.api.types.is_datetime64_any_dtype(
        df["created_at"]
    )


    # invalid-date should become NaT
    assert (
        df.loc[
            df["customer_id"] == 2,
            "created_at"
        ].iloc[0]
        is pd.NaT
    )


    # -------------------------------
    # Test duplicates removed
    # -------------------------------

    assert len(df) == 2