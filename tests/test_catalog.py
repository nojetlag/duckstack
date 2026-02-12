"""Tests for dataset catalog endpoints.

These tests run without a PostgreSQL database — they verify that
endpoints return 503 when DATABASE_URL is not configured.
"""

from fastapi.testclient import TestClient

from duckstack.main import app

client = TestClient(app)


def test_list_datasets_returns_503_without_database():
    resp = client.get("/datasets")
    assert resp.status_code == 503
    assert "DATABASE_URL" in resp.json()["detail"]


def test_create_dataset_returns_503_without_database():
    resp = client.post(
        "/datasets",
        json={"name": "test", "path": "sample.parquet"},
    )
    assert resp.status_code == 503


def test_get_dataset_returns_503_without_database():
    resp = client.get("/datasets/test")
    assert resp.status_code == 503


def test_delete_dataset_returns_503_without_database():
    resp = client.delete("/datasets/test")
    assert resp.status_code == 503
