import pytest
import pandas as pd
import io


class FakeObject:
    def __init__(self, object_name):
        self.object_name = object_name


class FakeResponse:
    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data

    def close(self):
        pass

    def release_conn(self):
        pass


class FakeMinioClient:

    def __init__(self):
        self.files = {}
        self.uploaded = {}
        self.deleted = []

    def list_objects(
        self,
        bucket_name,
        prefix,
        recursive
    ):

        return [
            FakeObject(name)
            for name in self.files.keys()
            if name.startswith(prefix)
        ]

    def get_object(self, bucket, object_name):
        return FakeResponse(
            self.files[object_name]
        )

    def remove_object(self, bucket, object_name):
        self.deleted.append(object_name)


@pytest.fixture
def fake_minio(monkeypatch):

    client = FakeMinioClient()

    # Create fake input parquet
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "product_name": [
                "Laptop",
                "Phone"
            ],
            "brand": [
                None,
                "Apple"
            ]
        }
    )

    buffer = io.BytesIO()

    df.to_parquet(
        buffer,
        engine="pyarrow",
        index=False
    )

    client.files[
        "raw/postgres/products/products_test.parquet"
    ] = buffer.getvalue()

    # fake upload function
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
        "pipeline.utils.upload_to_minio",
        fake_upload
    )


    return client


@pytest.fixture
def fake_mongodb_minio():

    client = FakeMinioClient()


    df = pd.DataFrame(
        {
            "customer_id": [1, 1, 2],
            "product_id": [100, 100, 200],
            "rating": [5, 5, 4],
            "helpful_votes": [3, 3, 2],

            "verified_purchase": [
                False,
                False,
                True
            ],

            "review_text": [
                "Good product",
                "Good product",
                "Average"
            ],

            "title": [
                "Nice",
                "Nice",
                "Okay"
            ],

            "created_at": [
                "2026-01-01",
                "2026-01-01",
                "invalid-date"
            ]
        }
    )



    buffer = io.BytesIO()

    df.to_parquet(
        buffer,
        engine="pyarrow",
        index=False
    )


    client.files[
        "raw/mongodb/product_reviews/reviews_test.parquet"
    ] = buffer.getvalue()


    return client