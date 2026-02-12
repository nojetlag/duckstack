"""Generate sample parquet file for development."""

from pathlib import Path

import duckdb

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

conn = duckdb.connect()
conn.execute(f"""
    COPY (
        SELECT * FROM (VALUES
            (1, 'Alice',   'Engineering', 95000),
            (2, 'Bob',     'Engineering', 90000),
            (3, 'Charlie', 'Marketing',   75000),
            (4, 'Diana',   'Marketing',   78000),
            (5, 'Eve',     'Sales',       70000),
            (6, 'Frank',   'Sales',       72000),
            (7, 'Grace',   'Engineering', 105000),
            (8, 'Heidi',   'Marketing',   80000)
        ) AS t(id, name, department, salary)
    ) TO '{DATA_DIR}/sample.parquet' (FORMAT PARQUET)
""")
print(f"Wrote {DATA_DIR / 'sample.parquet'}")
