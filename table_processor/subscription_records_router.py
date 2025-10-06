from supabase import create_client, Client
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from dotenv import load_dotenv
import logging
import os

from core.encryption import encrypt_data, decrypt_data

load_dotenv()

url: str = os.getenv("SUPABASE_URL") or ""
key: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
supabase: Client = create_client(url, key)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/subscription-records", tags=["subscription_records表操作"])

class GetRequest(BaseModel):
    user_id: str  # 必填
    ind: Optional[int] = None  # 精确查询
    status: Optional[str] = None
    start_time: Optional[str] = None  # YYYY-MM-DD
    end_time: Optional[str] = None    # YYYY-MM-DD
    limit: Optional[int] = 10
    offset: Optional[int] = 0

class UpdateRequest(BaseModel):
    ind: int = Field(..., description="记录唯一标识")
    user_id: str = Field(..., description="用户ID")

    seller_name: Optional[str] = Field(default=None, description="服务商名称，例如：OpenAI, Notion, Cursor 等")
    plan_name: Optional[str] = Field(default=None, description="订阅套餐名称，例如：Pro Plan, Business Plan 等")
    billing_cycle: Optional[str] = Field(default=None, description="计费周期：monthly, quarterly, yearly, one-time")
    amount: Optional[float] = Field(default=None, description="订阅金额")
    currency: Optional[str] = Field(default=None, description="货币类型，例如：USD, EUR, CNY")
    start_date: Optional[str] = Field(default=None, description="订阅开始日期，格式 YYYY-MM-DD")
    next_renewal_date: Optional[str] = Field(default=None, description="下次续费日期，格式 YYYY-MM-DD")
    end_date: Optional[str] = Field(default=None, description="订阅结束日期，格式 YYYY-MM-DD")
    status: Optional[str] = Field(default=None, description="订阅状态：active, expiring, expired")
    source: Optional[str] = Field(default=None, description="订阅来源，例如：web, email")
    note: Optional[str] = Field(default=None, description="备注或系统识别说明")


class DeleteRequest(BaseModel):
    user_id: str
    inds: List[int]

@router.post("/get-subscriptions")
def get_subscriptions(request: GetRequest):
    """
    查询订阅记录
    """
    logger.info(f"Querying subscriptions for user: {request.user_id}")
    
    try:
        query = supabase.table("subscription_records").select("*").eq("user_id", request.user_id)
        
        if request.ind:
            query = query.eq("ind", request.ind)
        
        elif request.status:
            query = query.eq("status", request.status)
        
        elif request.start_date != "string" and request.end_date != "string":
            start_dt = datetime.strptime(request.start_date, "%Y-%m-%d")
            query = query.gte("created_at", start_dt.isoformat())

            end_dt = datetime.strptime(request.end_date, "%Y-%m-%d")
            end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
            query = query.lte("created_at", end_dt.isoformat())
        
        else:
            query = query.order("created_at", desc=False).range(request.offset, request.offset + request.limit - 1)
        
        result = query.execute()
        
        if not result.data:
            return {"message": "No records found", "data": [], "total": 0, "status": "success"}
        
        # 解密敏感字段
        decrypted_result = [decrypt_data("subscription_records", record) for record in result.data]
        
        logger.info(f"Found {len(decrypted_result)} subscription records")
        return  {"message": "Query success", "data": decrypted_result, "total": len(decrypted_result), "status": "success"}
        
    except Exception as e:
        logger.exception(f"Failed to query subscriptions: {str(e)}")
        raise

@router.post("/get-subscription-stats")
def get_subscription_stats(user_id: str, year: int = None) -> dict:
    """
    获取订阅统计信息
    
    Returns:
        dict: 包含年度、月度、平均支出等统计数据
    """
    logger.info(f"Generating subscription statistics for user: {user_id}")
    
    try:
        if not year:
            year = datetime.now().year
        
        # 查询所有活跃订阅
        result = supabase.table("subscription_records").select("*").eq("user_id", user_id).in_("status", ["active", "expiring"]).execute()
        
        if not result.data:
            return {
                "total_active": 0,
                "total_expiring": 0,
                "annual_cost": 0,
                "monthly_average": 0,
                "by_currency": {},
                "by_cycle": {},
                "upcoming_renewals": []
            }
        
        # 解密数据
        subscriptions = [decrypt_data("subscription_records", record) for record in result.data]
        
        # 统计计算
        total_active = sum(1 for s in subscriptions if s.get("status") == "active")
        total_expiring = sum(1 for s in subscriptions if s.get("status") == "expiring")
        
        # 按货币统计
        by_currency = {}
        for sub in subscriptions:
            currency = sub.get("currency", "USD")
            amount = sub.get("amount", 0)
            cycle = sub.get("billing_cycle", "monthly")
            
            # 转换为年度成本
            annual = calculate_annual_cost(amount, cycle)
            
            if currency not in by_currency:
                by_currency[currency] = 0
            by_currency[currency] += annual
        
        # 按周期统计
        by_cycle = {}
        for sub in subscriptions:
            cycle = sub.get("billing_cycle", "monthly")
            if cycle not in by_cycle:
                by_cycle[cycle] = 0
            by_cycle[cycle] += 1
        
        # 即将续费（未来30天）
        today = datetime.now().date()
        upcoming = []
        for sub in subscriptions:
            next_renewal = sub.get("next_renewal_date")
            if next_renewal:
                renewal_date = datetime.fromisoformat(next_renewal).date()
                days_until = (renewal_date - today).days
                if 0 <= days_until <= 30:
                    upcoming.append({
                        "seller_name": sub.get("seller_name"),
                        "plan_name": sub.get("plan_name"),
                        "amount": sub.get("amount"),
                        "currency": sub.get("currency"),
                        "renewal_date": next_renewal,
                        "days_until": days_until
                    })
        
        upcoming.sort(key=lambda x: x["days_until"])
        
        # 月均支出（默认使用第一个货币）
        primary_currency = list(by_currency.keys())[0] if by_currency else "USD"
        annual_cost = by_currency.get(primary_currency, 0)
        monthly_average = round(annual_cost / 12, 2)
        
        return {
            "total_active": total_active,
            "total_expiring": total_expiring,
            "annual_cost": annual_cost,
            "monthly_average": monthly_average,
            "by_currency": by_currency,
            "by_cycle": by_cycle,
            "upcoming_renewals": upcoming[:10]  # 最多返回10个
        }
        
    except Exception as e:
        logger.exception(f"Failed to generate subscription stats: {str(e)}")
        raise

@router.post("/update-subscription")
async def update_subscription(request: UpdateRequest):
    """根据 ind 和 user_id 更新 subscription_records 表"""
    try:
        update_data = {}
        for field, value in request.dict(exclude={'ind', 'user_id'}, by_alias=True).items():
            if value and value != "string":
                update_data[field] = value

        if not update_data:
            return {"message": "No data to update", "status": "success"}
        
        update_data["updated_at"] = datetime.utcnow().isoformat()
        encrypted_update_data = encrypt_data("subscription_records", update_data)

        result = (
            supabase.table("subscription_records")
            .update(encrypted_update_data)
            .eq("ind", request.ind)
            .eq("user_id", request.user_id)
            .execute()
        )

        if not result.data:
            return {"error": "No matching record found or no permission to update", "status": "error"}

        decrypted_result = [decrypt_data("subscription_records", record) for record in result.data]
        return {
            "message": "Subscription records updated successfully",
            "updated_records": len(result.data),
            "data": decrypted_result,
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to update subscription_records: {str(e)}")
        return {"error": f"Failed to update subscription_records: {str(e)}", "status": "error"}


@router.delete("/delete-subscriptions")
async def delete_subscriptions(request: DeleteRequest):
    """根据 ind 和 user_id 删除 subscription_records 表记录"""
    try:
        if not request.inds:
            return {"error": "inds list cannot be empty", "status": "error"}

        result = (
            supabase.table("subscription_records")
            .delete()
            .eq("user_id", request.user_id)
            .in_("ind", request.inds)
            .execute()
        )

        deleted_count = len(result.data) if result.data else 0
        return {
            "message": "Records deleted successfully",
            "deleted_count": deleted_count,
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to delete subscription_records: {str(e)}")
        return {"error": f"Failed to delete subscription_records: {str(e)}", "status": "error"}
    

def calculate_annual_cost(amount: float, cycle: str) -> float:
    """将订阅费用转换为年度成本"""
    if cycle == "monthly":
        return amount * 12
    elif cycle == "quarterly":
        return amount * 4
    elif cycle == "yearly":
        return amount
    else:  # one-time
        return 0


