import logging
from typing import Optional, List
from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException
from core.performance_monitor import timer
from table_processor.subscription_records.service import SubscriptionRecordsService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/subscription-records", tags=["subscription_records表操作"])

# ========== 请求模型 ==========

class GetRequest(BaseModel):
    user_id: str
    status: Optional[str] = Field(default=None, description="订阅状态：active / upcoming / expired")

class GetRawRequest(BaseModel):
    user_id: str
    ind: Optional[int] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    limit: Optional[int] = 0
    offset: Optional[int] = 0
    year: Optional[int] = None
    month: Optional[int] = None

class UpdateRequest(BaseModel):
    ind: int = Field(..., description="记录唯一标识")
    user_id: str = Field(..., description="用户ID")
    id: Optional[str] = Field(default=None, description="对应到receipt_items_en表的id字段")
    buyer_name: Optional[str] = Field(default=None, description="订阅人名称")
    seller_name: Optional[str] = Field(default=None, description="服务商名称")
    plan_name: Optional[str] = Field(default=None, description="订阅套餐名称")
    billing_cycle: Optional[str] = Field(default=None, description="计费周期")
    amount: Optional[float] = Field(default=None, description="订阅金额")
    currency: Optional[str] = Field(default=None, description="货币类型")
    start_date: Optional[str] = Field(default=None, description="订阅开始日期")
    next_renewal_date: Optional[str] = Field(default=None, description="下次续费日期")
    end_date: Optional[str] = Field(default=None, description="订阅结束日期")
    source: Optional[str] = Field(default=None, description="订阅来源")
    note: Optional[str] = Field(default=None, description="备注")

class InsertRequest(BaseModel):
    user_id: str = Field(..., description="用户ID")
    id: Optional[str] = Field(default=None)
    buyer_name: Optional[str] = Field(default=None)
    seller_name: Optional[str] = Field(default=None)
    plan_name: Optional[str] = Field(default=None)
    billing_cycle: Optional[str] = Field(default=None)
    amount: Optional[float] = Field(default=None)
    currency: Optional[str] = Field(default=None)
    start_date: Optional[str] = Field(default=None)
    next_renewal_date: Optional[str] = Field(default=None)
    end_date: Optional[str] = Field(default=None)
    source: Optional[str] = Field(default=None)
    note: Optional[str] = Field(default=None)

class DeleteRequest(BaseModel):
    user_id: str
    inds: List[int]


# ========== 查询接口 ==========

@router.post("/get-subscriptions")
@timer("get_subscriptions")
async def get_subscriptions(request: GetRequest):
    """
    查询订阅列表（智能分组，每个链只返回最新一期）
    功能:
    - 识别同一订阅的多期续订
    - 仅返回每个订阅链的最新一期
    - 自动计算剩余天数和状态
    """
    try:
        result = await SubscriptionRecordsService.get_subscriptions(
            user_id=request.user_id,
            status_filter=request.status
        )
        return result
    except Exception as e:
        logger.exception(f"Failed to query subscriptions: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/get-raw-subscriptions")
@timer("get_raw_subscriptions")
async def get_raw_subscriptions(request: GetRawRequest):
    """
    查询原始订阅记录（返回所有记录，不做智能分组）
    """
    try:
        result = await SubscriptionRecordsService.get_raw_subscriptions(
            user_id=request.user_id,
            ind=request.ind,
            start_date=request.start_date,
            end_date=request.end_date,
            year=request.year,
            month=request.month,
            limit=request.limit,
            offset=request.offset
        )
        return result
    except Exception as e:
        logger.exception(f"Failed to query raw subscriptions: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/get-subscription-stats")
@timer("get_subscription_stats")
async def get_subscription_stats(user_id: str, year: int = None) -> dict:
    """
    获取订阅统计（仅统计非过期）
    """
    try:
        result = await SubscriptionRecordsService.get_subscription_stats(
            user_id=user_id,
            year=year
        )
        return result
    except Exception as e:
        logger.exception(f"Failed to generate subscription stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== 更新接口 ==========

@router.post("/update-subscription")
@timer("update_subscription")
async def update_subscription(request: UpdateRequest):
    """根据 ind 和 user_id 更新 subscription_records 表"""
    try:
        # 提取更新字段
        update_data = {}
        for field, value in request.dict(exclude={'ind', 'user_id'}, by_alias=True).items():
            if value and value != "string":
                update_data[field] = value
        
        # 调用 Service 层
        result = await SubscriptionRecordsService.update_subscription(
            ind=request.ind,
            user_id=request.user_id,
            update_fields=update_data
        )
        
        return result
    
    except Exception as e:
        logger.exception(f"Failed to update subscription: {str(e)}")
        return {"error": f"Failed to update subscription: {str(e)}", "status": "error"}


# ========== 插入接口 ==========

@router.post("/insert-subscription")
@timer("insert_subscription")
async def insert_subscription(request: InsertRequest):
    """新增一条 subscription_records 记录"""
    try:
        # 提取插入字段
        insert_data = {}
        for field, value in request.dict(exclude={'user_id'}, by_alias=True).items():
            if value and value != "string":
                insert_data[field] = value
        
        # 调用 Service 层
        result = await SubscriptionRecordsService.insert_subscription(
            user_id=request.user_id,
            insert_fields=insert_data
        )
        
        return result
    
    except Exception as e:
        logger.exception(f"Failed to insert subscription: {str(e)}")
        return {"error": f"Failed to insert subscription: {str(e)}", "status": "error"}


# ========== 删除接口 ==========

@router.delete("/delete-subscriptions")
@timer("delete_subscriptions")
async def delete_subscriptions(request: DeleteRequest):
    """根据 inds 和 user_id 删除 subscription_records 表记录"""
    try:
        result = await SubscriptionRecordsService.delete_subscriptions(
            user_id=request.user_id,
            inds=request.inds
        )
        return result
    
    except Exception as e:
        logger.exception(f"Failed to delete subscriptions: {str(e)}")
        return {"error": f"Failed to delete subscriptions: {str(e)}", "status": "error"}
