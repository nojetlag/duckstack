from datetime import datetime

from pydantic import BaseModel


class QueryRequest(BaseModel):
    sql: str


class QueryResponse(BaseModel):
    columns: list[str]
    rows: list[list]
    row_count: int


class DatasetCreate(BaseModel):
    name: str
    path: str
    description: str = ""


class ColumnInfo(BaseModel):
    name: str
    dtype: str


class DatasetSummary(BaseModel):
    id: int
    name: str
    path: str
    description: str
    created_at: datetime


class DatasetDetail(BaseModel):
    id: int
    name: str
    path: str
    description: str
    created_at: datetime
    columns: list[ColumnInfo]
