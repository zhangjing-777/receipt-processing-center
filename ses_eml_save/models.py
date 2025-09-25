from pydantic import BaseModel, Field
from typing import Optional, List


class UpdateReceiptRequest(BaseModel):
    ind: int = Field(..., description="记录ID")
    user_id: str = Field(..., description="用户ID")
    
    buyer: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    seller: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    invoice_date: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    category: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    invoice_total: Optional[float] = Field(default=None, json_schema_extra={"default": None})
    currency: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    invoice_number: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    address: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    file_url: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    original_info: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    ocr: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    hash_id: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    create_time: Optional[str] = Field(default=None, json_schema_extra={"default": None})


class GetReceiptRequest(BaseModel):
    user_id: str  # 必填
    ind: Optional[int] = 0  # 可选，如果提供则精确查询单条记录
    limit: Optional[int] = 10  # 默认返回10条
    offset: Optional[int] = 0  # 默认从第0条开始


class DeleteReceiptRequest(BaseModel):
    user_id: str  # 必填
    inds: List[int]  # 必填，支持批量删除