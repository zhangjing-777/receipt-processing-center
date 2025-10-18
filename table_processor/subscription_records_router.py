from fastapi import APIRouter
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, timedelta, date
import logging
import asyncio
import calendar
import hashlib
from sqlalchemy import select, update, func, and_, insert
from core.database import AsyncSessionLocal
from core.models import SubscriptionRecords
from core.encryption import encrypt_data
from core.batch_operations import BatchOperations
from core.performance_monitor import timer, measure_time
from table_processor.utils import process_record

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/subscription-records", tags=["subscription_records表操作"])

# ========== 请求模型 ==========

class GetRequest(BaseModel):
    user_id: str
    status: Optional[str] = Field(default=None, description="订阅状态：active / upcoming / expired")

class GetRawRequest(BaseModel):
    user_id: str
    ind: Optional[int] = None
    status: Optional[str] = None
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
    status: Optional[str] = Field(default=None, description="订阅状态")
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
    status: Optional[str] = Field(default=None)
    source: Optional[str] = Field(default=None)
    note: Optional[str] = Field(default=None)

class DeleteRequest(BaseModel):
    user_id: str
    inds: List[int]
    
# get
@router.post("/get-subscriptions")
@timer("get_subscriptions")
async def get_subscriptions(request: GetRequest):
    """
    优化的订阅查询（支持多期续订识别）
    
    功能:
    - 识别同一订阅的多期续订
    - 仅返回每个订阅链的最新一期
    - 自动计算剩余天数和状态
    """
    logger.info(f"Querying subscriptions for user: {request.user_id}")

    try:
        today = datetime.utcnow().date()

        async with measure_time("database_query"):
            async with AsyncSessionLocal() as session:
                # 使用窗口函数找每个 chain 的最大 ind
                subquery = (
                    select(
                        SubscriptionRecords,
                        func.row_number().over(
                            partition_by=SubscriptionRecords.chain_key_bidx,
                            order_by=SubscriptionRecords.ind.desc()
                        ).label('rn')
                    )
                    .where(SubscriptionRecords.user_id == request.user_id)
                    .subquery()
                )
                
                # 只取每个 chain 的第一条（ind 最大的）
                query = select(subquery).where(subquery.c.rn == 1)
                
                result = await session.execute(query)
                records = result.mappings().all()

        if not records:
            return {"message": "No records found", "data": [], "total": 0, "status": "success"}

        logger.info(f"Found {len(records)} unique subscription chains")

        # 并行解密
        async with measure_time("decrypt_records"):
            decrypted_result = await asyncio.gather(
                *[process_record(r, "subscription_records") for r in records]
            )

        # 并行计算状态
        async with measure_time("calculate_status"):
            enriched_result = await asyncio.gather(
                *[_enrich_subscription(record, today) for record in decrypted_result]
            )

        # 按状态筛选
        if request.status and request.status != "string":
            view = request.status.lower()
            enriched_result = [
                r for r in enriched_result if r.get("status_label") == view
            ]

        return {
            "message": "Query success",
            "data": enriched_result,
            "total": len(enriched_result),
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to query subscriptions: {str(e)}")
        raise

async def _enrich_subscription(record: dict, today: date) -> dict:
    """
    异步计算订阅状态和剩余天数
    
    状态定义:
    - active: 当前生效中
    - upcoming: 7天内到期
    - expired: 已过期
    """
    start_date = record.get("start_date")
    end_date = record.get("end_date")

    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    days_left = max((end_date - today).days, 0) if end_date else 0
    days_expired = max((today - end_date).days, 0) if end_date else 0

    if end_date and end_date < today:
        status_label = "expired"
    elif end_date and end_date <= today + timedelta(days=7):
        status_label = "upcoming"
    else:
        status_label = "active"

    record.update({
        "days_left": days_left,
        "days_expired": days_expired,
        "status_label": status_label
    })
    
    return record


# ========== 原始查询接口 ==========

@router.post("/get-raw-subscriptions")
@timer("get_raw_subscriptions")
async def get_raw_subscriptions(request: GetRawRequest):
    """
    优化的原始查询接口（返回所有记录，不做智能分组）
    """
    logger.info(f"Querying raw subscriptions for user: {request.user_id}")
    
    try:
        async with measure_time("database_query"):
            async with AsyncSessionLocal() as session:
                query = select(SubscriptionRecords).where(
                    SubscriptionRecords.user_id == request.user_id
                )
                
                if request.ind:
                    query = query.where(SubscriptionRecords.ind == request.ind)
                
                elif request.status != "string":
                    query = query.where(SubscriptionRecords.status == request.status)
                
                elif request.start_date != "string" and request.end_date != "string":
                    start_dt = datetime.strptime(request.start_date, "%Y-%m-%d").date()
                    query = query.where(SubscriptionRecords.start_date >= start_dt)

                    end_dt = datetime.strptime(request.end_date, "%Y-%m-%d").date()
                    query = query.where(SubscriptionRecords.start_date <= end_dt)
                
                elif request.offset and request.limit:
                    query = query.order_by(
                        SubscriptionRecords.start_date.desc()
                    ).offset(request.offset).limit(request.limit)
                
                elif request.year and request.month:               
                    year, month = request.year, request.month
                    start_dt = datetime(year, month, 1)
                    _, last_day = calendar.monthrange(year, month)
                    end_dt = datetime(year, month, last_day, 23, 59, 59, 999999)
                    query = query.where(
                        SubscriptionRecords.start_date >= start_dt,
                        SubscriptionRecords.start_date <= end_dt
                    )
                    logger.info(f"Monthly query: {year}-{month:02d}")

                else:
                    now = datetime.utcnow()
                    start_of_year = datetime(now.year, 1, 1)
                    end_of_year = datetime(now.year, 12, 31, 23, 59, 59, 999999)
                    query = query.where(
                        SubscriptionRecords.start_date >= start_of_year,
                        SubscriptionRecords.start_date <= end_of_year
                    )
                    logger.info(f"Default: current year ({start_of_year.date()} ~ {end_of_year.date()})")

                query = query.order_by(SubscriptionRecords.start_date.desc())
                result = await session.execute(query)
                records = result.mappings().all()
        
        if not records:
            return {"message": "No records found", "data": [], "total": 0, "status": "success"}
        
        # 并行解密
        async with measure_time("decrypt_records"):
            decrypted_result = await asyncio.gather(
                *[process_record(r, "subscription_records") for r in records]
            )
        
        logger.info(f"Found {len(decrypted_result)} subscription records")
        return {
            "message": "Query success",
            "data": decrypted_result,
            "total": len(decrypted_result),
            "status": "success"
        }
        
    except Exception as e:
        logger.exception(f"Failed to query raw subscriptions: {str(e)}")
        raise


# ========== 统计接口 ==========

@router.post("/get-subscription-stats")
@timer("get_subscription_stats")
async def get_subscription_stats(user_id: str, year: int = None) -> dict:
    """
    优化的订阅统计接口（仅统计 active 状态）
    
    包含：
    - 概览（总数、年度成本、月均成本）
    - 按币种分组
    - 按计费周期分组
    """
    logger.info(f"Generating subscription statistics for user: {user_id}")
    
    try:
        if not year:
            year = datetime.now().year

        # ✅ 查询 active 订阅
        async with measure_time("database_query"):
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(SubscriptionRecords).where(
                        and_(
                            SubscriptionRecords.user_id == user_id,
                            SubscriptionRecords.status == "active"
                        )
                    )
                )
                records = result.mappings().all()

        if not records:
            return {
                "overview": {
                    "total_active": 0,
                    "annual_costs_by_currency": {},
                    "monthly_average_by_currency": {}
                },
                "by_currency": [],
                "by_billing_cycle": []
            }

        # 并行解密
        async with measure_time("decrypt_records"):
            subscriptions = await asyncio.gather(
                *[process_record(r, "subscription_records") for r in records]
            )

        # 在线程池中执行统计计算（CPU密集）
        async with measure_time("calculate_stats"):
            stats = await asyncio.to_thread(
                _calculate_subscription_stats,
                subscriptions
            )

        return stats
        
    except Exception as e:
        logger.exception(f"Failed to generate subscription stats: {str(e)}")
        raise


def _calculate_subscription_stats(subscriptions: List[dict]) -> dict:
    """
    计算订阅统计（同步函数，在线程池中执行）
    """
    total_active = len(subscriptions)
    currency_stats = {}
    by_cycle = {}

    # 按货币统计
    for s in subscriptions:
        currency = s.get("currency", "USD")
        amount = float(s.get("amount") or 0)
        cycle = s.get("billing_cycle", "monthly")
        annual_cost = _calculate_annual_cost(amount, cycle)

        if currency not in currency_stats:
            currency_stats[currency] = {
                "annual_total": 0.0,
                "monthly_avg": 0.0,
                "count": 0
            }

        currency_stats[currency]["annual_total"] += annual_cost
        currency_stats[currency]["count"] += 1

    # 计算月均
    for c in currency_stats.values():
        c["monthly_avg"] = round(c["annual_total"] / 12, 2)

    # 按计费周期统计
    for s in subscriptions:
        cycle = s.get("billing_cycle", "monthly")
        by_cycle[cycle] = by_cycle.get(cycle, 0) + 1

    # 组织返回结构
    by_currency_list = [
        {
            "currency": c,
            "annual_total": round(v["annual_total"], 2),
            "monthly_avg": v["monthly_avg"],
            "subscription_count": v["count"],
        }
        for c, v in sorted(
            currency_stats.items(),
            key=lambda x: x[1]["annual_total"],
            reverse=True
        )
    ]

    by_cycle_list = [
        {"cycle": k, "count": v}
        for k, v in by_cycle.items()
    ]

    overview = {
        "total_active": total_active,
        "annual_costs_by_currency": {
            k: round(v["annual_total"], 2)
            for k, v in currency_stats.items()
        },
        "monthly_average_by_currency": {
            k: v["monthly_avg"]
            for k, v in currency_stats.items()
        },
    }

    return {
        "overview": overview,
        "by_currency": by_currency_list,
        "by_billing_cycle": by_cycle_list,
    }


def _calculate_annual_cost(amount: float, cycle: str) -> float:
    """将订阅费用转换为年度成本"""
    if cycle == "monthly":
        return amount * 12
    elif cycle == "quarterly":
        return amount * 4
    elif cycle == "yearly":
        return amount
    else:  # one-time
        return 0


# ========== 更新接口 ==========

def calculate_chain_key_bidx(user_id: str, data: dict) -> str:
    """计算 chain_key_bidx hash 值"""
    hash_input = "|".join([
        str(user_id),
        str(data.get("buyer_name", "")),
        str(data.get("seller_name", "")),
        str(data.get("plan_name", "")),
        str(data.get("currency", "USD")),
        str(data.get("amount", 0))
    ])
    return hashlib.md5(hash_input.encode()).hexdigest()


@router.post("/update-subscription")
@timer("update_subscription")
async def update_subscription(request: UpdateRequest):
    """优化的更新接口 - 包含 hash 字段"""
    try:
        update_data = {}
        for field, value in request.dict(exclude={'ind', 'user_id'}, by_alias=True).items():
            if value and value != "string":
                update_data[field] = value

        if not update_data:
            return {"message": "No data to update", "status": "success"}
        
        async with AsyncSessionLocal() as session:
            # 1. 先查询现有记录
            query_result = await session.execute(
                select(SubscriptionRecords)
                .where(and_(
                    SubscriptionRecords.ind == request.ind,
                    SubscriptionRecords.user_id == request.user_id
                ))
            )
            existing_record = query_result.scalar_one_or_none()
            
            if not existing_record:
                return {"error": "No matching record found", "status": "error"}
            
            # 2. 解密现有数据
            existing_data = await process_record(
                existing_record.__dict__, 
                "subscription_records"
            )
            
            # 3. 合并数据：现有数据 + 更新数据
            merged_data = existing_data.copy()
            merged_data.update(update_data)
            
            # 4. 添加时间戳
            update_data["updated_at"] = datetime.utcnow()
            
            # 5. 用合并后的完整数据计算 hash
            chain_key_bidx = calculate_chain_key_bidx(request.user_id, merged_data)
            update_data["chain_key_bidx"] = chain_key_bidx
            
            # 6. 加密更新数据
            encrypted_update_data = encrypt_data("subscription_records", update_data)

            # 7. 执行更新
            async with measure_time("database_update"):
                result = await session.execute(
                    update(SubscriptionRecords)
                    .where(and_(
                        SubscriptionRecords.ind == request.ind,
                        SubscriptionRecords.user_id == request.user_id
                    ))
                    .values(**encrypted_update_data)
                    .returning(SubscriptionRecords)
                )
                await session.commit()
                updated_records = result.mappings().all()

            if not updated_records:
                return {"error": "Update operation failed", "status": "error"}

            # 8. 并行解密结果
            decrypted_result = await asyncio.gather(
                *[process_record(r, "subscription_records") for r in updated_records]
            )
            
            return {
                "message": "Subscription updated successfully",
                "updated_records": len(decrypted_result),
                "data": decrypted_result,
                "status": "success"
            }

    except Exception as e:
        logger.exception(f"Failed to update subscription: {str(e)}")
        return {"error": f"Failed to update subscription: {str(e)}", "status": "error"}
    
# ========== 插入接口 ==========

@router.post("/insert-subscription")
@timer("insert_subscription")
async def insert_subscription(request: InsertRequest):
    """优化的插入接口 - 包含 hash 字段"""
    try:
        insert_data = {}
        for field, value in request.dict(exclude={'user_id'}, by_alias=True).items():
            if value and value != "string":
                insert_data[field] = value

        if not insert_data:
            return {"message": "No data provided", "status": "success"}

        # 时间戳处理
        now_utc = datetime.utcnow()
        insert_data["updated_at"] = now_utc
        if "created_at" not in insert_data:
            insert_data["created_at"] = now_utc

        # 计算并添加 hash 字段（insert 时所有字段都是新的，直接用 insert_data）
        chain_key_bidx = calculate_chain_key_bidx(request.user_id, insert_data)
        insert_data["chain_key_bidx"] = chain_key_bidx
        
        # 加密
        encrypted_data = encrypt_data("subscription_records", insert_data)
        encrypted_data["user_id"] = request.user_id
        
        async with measure_time("database_insert"):
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    insert(SubscriptionRecords)
                    .values([encrypted_data])
                    .returning(SubscriptionRecords)
                )
                await session.commit()
                inserted_records = result.mappings().all()

        if not inserted_records:
            return {"message": "Insert failed", "status": "error"}
        
        # 并行解密
        decrypted_result = await asyncio.gather(
            *[process_record(r, "subscription_records") for r in inserted_records]
        )
        
        return {
            "message": "Subscription inserted successfully",
            "affected_records": len(decrypted_result),
            "data": decrypted_result,
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to insert subscription: {str(e)}")
        return {"error": f"Failed to insert subscription: {str(e)}", "status": "error"}

# ========== 删除接口 ==========

@router.delete("/delete-subscriptions")
@timer("delete_subscriptions")
async def delete_subscriptions(request: DeleteRequest):
    """优化的批量删除接口"""
    try:
        if not request.inds:
            return {"error": "inds list cannot be empty", "status": "error"}

        batch_ops = BatchOperations()
        
        async with measure_time("batch_delete"):
            deleted_count = await batch_ops.batch_delete(
                SubscriptionRecords,
                request.inds,
                key_field='ind'
            )

        return {
            "message": "Records deleted successfully",
            "deleted_count": deleted_count,
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to delete subscriptions: {str(e)}")
        return {"error": f"Failed to delete subscriptions: {str(e)}", "status": "error"}