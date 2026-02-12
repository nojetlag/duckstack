# Duckstack

Unified Data Access Layer — query local and S3 Parquet files via a REST API, powered by DuckDB.

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

## S3 Support

Set AWS credentials via environment variables to query remote parquet files:

```bash
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export AWS_REGION=us-east-1  # default

uvicorn duckstack.main:app --reload
```

Then query S3 paths directly:

```bash
curl -s localhost:8000/query \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM '\''s3://my-bucket/data/events.parquet'\'' LIMIT 10"}' | python -m json.tool
```

Without credentials, local parquet queries still work normally.

## Tests

```bash
pytest
```

## Roadmap

- PostgreSQL catalog for dataset metadata
- Auth and query allow-listing
