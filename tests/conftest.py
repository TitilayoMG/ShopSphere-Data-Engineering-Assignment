
import io
import pandas as pd
import pytest
from pipeline import transform
from unittest.mock import Mock


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
            for name in self.files
            if name.startswith(prefix)
        ]

    def get_object(self, bucket_name, object_name):
        return FakeResponse(
            self.files[object_name]
        )

    def remove_object(self, bucket, object_name):
        self.deleted.append(object_name)

    def add_parquet(self, path, df):
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)                  # rewind
        self.files[path] = buffer.read()

    def upload_object(self, object_name, buffer):
        buffer.seek(0)
        self.uploaded[object_name] = buffer.read()


@pytest.fixture
def fake_upload():
    def _fake_upload(client, bucket, object_name, buffer, content_type):
        client.upload_object(object_name, buffer)

    return _fake_upload


# OR fake_upload() can also be written like this
# @pytest.fixture
# def fake_upload():
#     def _fake_upload(
#         client,
#         bucket,
#         object_name,
#         buffer,
#         content_type,
#     ):
#         buffer.seek(0)
#         client.uploaded[object_name] = buffer.read()

#     return _fake_upload

@pytest.fixture
def fake_minio():
    client = FakeMinioClient()
    bucket = "test-bucket"

    def add_file(path, df):
        if isinstance(df, pd.DataFrame):
            client.add_parquet(path, df)
        else:
            client.files[path] = df
        return client, bucket

    return add_file


@pytest.fixture
def setup_transform(monkeypatch, fake_upload):
    def _setup(client, bucket):
        monkeypatch.setattr(
            transform,
            "get_minio_client",
            lambda: (client, bucket),
        )
        monkeypatch.setattr(
            transform,
            "upload_to_minio",
            fake_upload,
        )
        monkeypatch.setattr(
            transform,
            "DataQualityValidator",
            Mock(),
        )

    return _setup




















# @pytest.fixture
# def fake_mongodb_minio():

#     client = FakeMinioClient()


#     df = pd.DataFrame(
#         {
#             "customer_id": [1, 1, 2],
#             "product_id": [100, 100, 200],
#             "rating": [5, 5, 4],
#             "helpful_votes": [3, 3, 2],

#             "verified_purchase": [
#                 False,
#                 False,
#                 True
#             ],

#             "review_text": [
#                 "Good product",
#                 "Good product",
#                 "Average"
#             ],

#             "title": [
#                 "Nice",
#                 "Nice",
#                 "Okay"
#             ],

#             "created_at": [
#                 "2026-01-01",
#                 "2026-01-01",
#                 "invalid-date"
#             ]
#         }
#     )



#     buffer = io.BytesIO()

#     df.to_parquet(
#         buffer,
#         engine="pyarrow",
#         index=False
#     )


#     client.files[
#         "raw/mongodb/product_reviews/reviews_test.parquet"
#     ] = buffer.getvalue()


#     return client