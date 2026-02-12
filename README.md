# Duckstack

Unified Data Access Layer — query local Parquet files via a REST API, powered by DuckDB.

## Quickstart

```bash
# install
pip install -e ".[dev]"

# generate sample data
python scripts/seed_parquet.py

# run
uvicorn duckstack.main:app --reload

# try it
curl -s localhost:8000/query \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM '\''sample.parquet'\''"}' | python -m json.tool
```

## Endpoints

| Method | Path      | Description                       |
|--------|-----------|-----------------------------------|
| GET    | `/health` | Liveness check                    |
| POST   | `/query`  | Execute SQL, returns JSON result  |

### POST /query

```json
{ "sql": "SELECT department, AVG(salary) FROM 'sample.parquet' GROUP BY department" }
```

## Tests

```bash
pytest
```

## Roadmap

- S3-backed Parquet reads (`s3://bucket/path/*.parquet`)
- PostgreSQL catalog for dataset metadata
- Auth and query allow-listing
