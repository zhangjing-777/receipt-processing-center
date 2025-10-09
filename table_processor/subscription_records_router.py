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
    start_date: Optional[str] = None  # YYYY-MM-DD
    end_date: Optional[str] = None    # YYYY-MM-DD
    limit: Optional[int] = 10
    offset: Optional[int] = 0

class UpdateRequest(BaseModel):
    ind: int = Field(..., description="记录唯一标识")
    user_id: str = Field(..., description="用户ID")

    id: Optional[str] = Field(default=None, description="对应到receipt_items_en表的id字段")
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


class InsertRequest(BaseModel):
    user_id: str = Field(..., description="用户ID")

    id: Optional[str] = Field(default=None, description="对应到receipt_items_en表的id字段")
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
async def get_subscriptions(request: GetRequest):
    """
    查询订阅记录
    """
    logger.info(f"Querying subscriptions for user: {request.user_id}")
    
    try:
        query = supabase.table("subscription_records").select("*").eq("user_id", request.user_id)
        
        if request.ind:
            query = query.eq("ind", request.ind)
        
        elif request.status != "string":
            query = query.eq("status", request.status)
        
        elif request.start_date != "string" and request.end_date != "string":
            start_dt = datetime.strptime(request.start_date, "%Y-%m-%d")
            query = query.gte("start_date", start_dt.isoformat())

            end_dt = datetime.strptime(request.end_date, "%Y-%m-%d")
            end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
            query = query.lte("start_date", end_dt.isoformat())
        
        else:
            query = query.order("start_date", desc=True).range(request.offset, request.offset + request.limit - 1)
        
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
async def get_subscription_stats(user_id: str, year: int = None) -> dict:
    """
    获取订阅统计信息（仅统计 active 状态）
    包含：概览、按币种支出、按计费周期分布
    """
    logger.info(f"Generating subscription statistics for user: {user_id}")
    
    try:
        if not year:
            year = datetime.now().year

        # ✅ 仅查询 active 订阅
        result = (
            supabase.table("subscription_records")
            .select("*")
            .eq("user_id", user_id)
            .eq("status", "active")
            .execute()
        )

        if not result.data:
            return {
                "overview": {
                    "total_active": 0,
                    "annual_costs_by_currency": {},
                    "monthly_average_by_currency": {}
                },
                "by_currency": [],
                "by_billing_cycle": []
            }

        subscriptions = [decrypt_data("subscription_records", r) for r in result.data]

        # === 📊 概览 ===
        total_active = len(subscriptions)

        # === 💶 按货币统计 ===
        currency_stats = {}
        for s in subscriptions:
            currency = s.get("currency", "USD")
            amount = float(s.get("amount") or 0)
            cycle = s.get("billing_cycle", "monthly")
            annual_cost = calculate_annual_cost(amount, cycle)

            if currency not in currency_stats:
                currency_stats[currency] = {"annual_total": 0.0, "monthly_avg": 0.0, "count": 0}

            currency_stats[currency]["annual_total"] += annual_cost
            currency_stats[currency]["count"] += 1

        for c in currency_stats.values():
            c["monthly_avg"] = round(c["annual_total"] / 12, 2)

        by_currency_list = [
            {
                "currency": c,
                "annual_total": round(v["annual_total"], 2),
                "monthly_avg": v["monthly_avg"],
                "subscription_count": v["count"],
            }
            for c, v in sorted(currency_stats.items(), key=lambda x: x[1]["annual_total"], reverse=True)
        ]

        # === ⏱️ 按计费周期统计 ===
        by_cycle = {}
        for s in subscriptions:
            cycle = s.get("billing_cycle", "monthly")
            by_cycle[cycle] = by_cycle.get(cycle, 0) + 1
        by_cycle_list = [{"cycle": k, "count": v} for k, v in by_cycle.items()]

        # === 组织返回结构 ===
        overview = {
            "total_active": total_active,
            "annual_costs_by_currency": {k: round(v["annual_total"], 2) for k, v in currency_stats.items()},
            "monthly_average_by_currency": {k: v["monthly_avg"] for k, v in currency_stats.items()},
        }

        return {
            "overview": overview,
            "by_currency": by_currency_list,
            "by_billing_cycle": by_cycle_list,
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

@router.post("/insert-subscription")
async def insert_subscription(request: InsertRequest):
    """
    新增一条 subscription_records 记录（数据库自动自增 ind）
    """
    try:
        # Step 1️⃣ 提取有效字段
        update_data = {}
        for field, value in request.dict(exclude={'ind', 'user_id'}, by_alias=True).items():
            if value and value != "string":
                update_data[field] = value

        if not update_data:
            return {"message": "No data provided", "status": "success"}

        # Step 2️⃣ 时间戳处理
        now_utc = datetime.utcnow().isoformat()
        update_data["updated_at"] = now_utc
        if "created_at" not in update_data:
            update_data["created_at"] = now_utc

        # Step 3️⃣ 加密字段
        encrypted_data = encrypt_data("subscription_records", update_data)

        # ✅ 新增逻辑（数据库自动自增 ind）
        encrypted_data["user_id"] = request.user_id
        result = (
            supabase.table("subscription_records")
            .insert(encrypted_data)
            .execute()
        )

        if not result.data:
            return {"message": "Insert failed", "status": "error"}

        decrypted_result = [decrypt_data("subscription_records", r) for r in result.data]
        return {
            "message": "New subscription record inserted successfully",
            "affected_records": len(result.data),
            "data": decrypted_result,
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to insert subscription_records: {str(e)}")
        return {
            "error": f"Failed to insert subscription_records: {str(e)}",
            "status": "error"
        }


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


