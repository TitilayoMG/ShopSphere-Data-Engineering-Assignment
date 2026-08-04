import pytest 
from unittest.mock import MagicMock

@pytest.fixture
def mock_minio(monkeypatch):
    fake_client = MagicMock()
    fake_bucket = "test-bucket"

    get_client = MagicMock(return_value=(fake_client, fake_bucket))
    upload = MagicMock()

    monkeypatch.setattr(
        "pytest_lessons.full_transform_pytest.get_minio_client",
        get_client,
    )

    monkeypatch.setattr(
        "pytest_lessons.full_transform_pytest.upload_to_minio",
        upload,
    )

    return fake_client, fake_bucket, get_client, upload