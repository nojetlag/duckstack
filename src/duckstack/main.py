from pathlib import Path

import duckdb
from fastapi import FastAPI, HTTPException

from duckstack.config import settings
from duckstack.schemas import QueryRequest, QueryResponse

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

db = duckdb.connect()
db.execute(f"SET file_search_path = '{DATA_DIR}'")

# S3 support via httpfs
db.execute("INSTALL httpfs")
db.execute("LOAD httpfs")
if settings.aws_access_key_id:
    db.execute(f"SET s3_access_key_id = '{settings.aws_access_key_id}'")
    db.execute(f"SET s3_secret_access_key = '{settings.aws_secret_access_key}'")
    db.execute(f"SET s3_region = '{settings.aws_region}'")

app = FastAPI(title="Duckstack", version="0.1.0")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest):
    try:
        result = db.execute(req.sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return QueryResponse(
            columns=columns,
            rows=[list(r) for r in rows],
            row_count=len(rows),
        )
    except duckdb.Error as e:
        raise HTTPException(status_code=400, detail=str(e))
