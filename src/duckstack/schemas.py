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


# --- API Sources ---


class ApiSourceCreate(BaseModel):
    name: str
    endpoint_url: str
    query_params: dict[str, str] = {}
    auth_header: str = ""
    auth_env_var: str = ""
    api_key_param: str = ""
    api_key_override: str = ""
    response_path: str = "results"
    ttl_seconds: int = 300
    description: str = ""


class ApiSourceSummary(BaseModel):
    id: int
    name: str
    endpoint_url: str
    description: str
    ttl_seconds: int
    created_at: datetime


class ApiSourceDetail(ApiSourceSummary):
    query_params: dict[str, str]
    auth_header: str
    auth_env_var: str
    api_key_param: str
    response_path: str


class ApiQueryRequest(BaseModel):
    source: str
    params: dict[str, str] = {}
    sql: str = ""


class ApiQueryResponse(QueryResponse):
    cached: bool = False
    source_name: str = ""
