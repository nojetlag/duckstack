from fastapi.testclient import TestClient

from duckstack.main import app

client = TestClient(app)


def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_query_sample_parquet():
    resp = client.post("/query", json={"sql": "SELECT * FROM 'sample.parquet' ORDER BY id"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["columns"] == ["id", "name", "department", "salary"]
    assert body["row_count"] == 8
    assert body["rows"][0] == [1, "Alice", "Engineering", 95000]


def test_query_aggregate():
    resp = client.post(
        "/query",
        json={"sql": "SELECT department, AVG(salary)::INT AS avg_salary FROM 'sample.parquet' GROUP BY department ORDER BY department"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["row_count"] == 3


def test_query_invalid_sql():
    resp = client.post("/query", json={"sql": "SELECT * FROM nonexistent_table"})
    assert resp.status_code == 400


def test_query_s3_path_returns_error_without_real_bucket():
    resp = client.post("/query", json={"sql": "SELECT * FROM 's3://no-such-bucket/file.parquet'"})
    assert resp.status_code == 400
