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

| Method | Path               | Description                                    |
|--------|--------------------|------------------------------------------------|
| GET    | `/health`          | Liveness check                                 |
| POST   | `/query`           | Execute SQL, returns JSON result               |
| GET    | `/datasets`        | List all registered datasets                   |
| POST   | `/datasets`        | Register a dataset (auto-infers column schema) |
| GET    | `/datasets/{name}` | Get dataset detail including columns           |
| DELETE | `/datasets/{name}` | Unregister a dataset                           |

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

## Catalog

Duckstack includes an optional PostgreSQL-backed catalog for registering datasets with logical names.
When configured, the catalog auto-infers column schemas from parquet files on registration.

### Setup

Set the `DATABASE_URL` environment variable to enable the catalog:

```bash
export DATABASE_URL=postgresql://user:pass@localhost:5432/duckstack
uvicorn duckstack.main:app --reload
```

The required tables (`datasets` and `dataset_columns`) are created automatically on startup.

Without `DATABASE_URL`, the catalog endpoints return `503 Service Unavailable`.

### Usage

Register a dataset:

```bash
curl -X POST localhost:8000/datasets \
  -H 'Content-Type: application/json' \
  -d '{"name": "employees", "path": "sample.parquet", "description": "Employee records"}'
```

List all datasets:

```bash
curl localhost:8000/datasets
```

Get dataset detail with column schema:

```bash
curl localhost:8000/datasets/employees
```

Delete a dataset:

```bash
curl -X DELETE localhost:8000/datasets/employees
```

## Tests

```bash
pytest
```

## Roadmap

- Auth and query allow-listing
