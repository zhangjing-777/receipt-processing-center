import logging
import asyncio
import hashlib
import calendar
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date, timedelta
from decimal import Decimal
from sqlalchemy import select, update, insert, delete, and_, func
from core.database import AsyncSessionLocal
from core.models import SubscriptionRecords, CanonicalEntities
from core.encryption import encrypt_data
from core.canonicalization import (
    normalize_subscription_fields,
    generate_normalized_key,
    invalidate_canonical_cache
)
from core.batch_operations import BatchOperations
from core.performance_monitor import measure_time
from table_processor.utils import process_record

logger = logging.getLogger(__name__)


class SubscriptionRecordsService:
    """订阅记录业务逻辑层"""
    
    @staticmethod
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
    
    @staticmethod
    async def get_subscriptions(
        user_id: str,
        status_filter: Optional[str] = None
    ) -> Dict:
        """
        查询订阅列表（智能分组，每个链只返回最新一期）
        
        Args:
            user_id: 用户 ID
            status_filter: 状态过滤（active/upcoming/expired）
            
        Returns:
            订阅列表
        """
        logger.info(f"Querying subscriptions for user: {user_id}")
        
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
                    .where(SubscriptionRecords.user_id == user_id)
                    .subquery()
                )
                
                query = select(subquery).where(subquery.c.rn == 1)
                result = await session.execute(query)
                records = result.mappings().all()
        
        if not records:
            return {
                "message": "No records found",
                "data": [],
                "total": 0,
                "status": "success"
            }
        
        logger.info(f"Found {len(records)} unique subscription chains")
        
        # 并行解密
        async with measure_time("decrypt_records"):
            decrypted_result = await asyncio.gather(
                *[process_record(r, "subscription_records") for r in records]
            )
        
        # 并行计算状态
        async with measure_time("calculate_status"):
            enriched_result = await asyncio.gather(
                *[SubscriptionRecordsService._enrich_subscription(record, today) 
                  for record in decrypted_result]
            )
        
        # 按状态筛选
        if status_filter and status_filter != "string":
            view = status_filter.lower()
            enriched_result = [
                r for r in enriched_result if r.get("status_label") == view
            ]
        
        return {
            "message": "Query success",
            "data": enriched_result,
            "total": len(enriched_result),
            "status": "success"
        }
    
    @staticmethod
    async def get_raw_subscriptions(
        user_id: str,
        ind: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        year: Optional[int] = None,
        month: Optional[int] = None,
        limit: int = 0,
        offset: int = 0
    ) -> Dict:
        """
        查询原始订阅记录（返回所有记录，不做智能分组）
        
        Args:
            user_id: 用户 ID
            ind: 精确查询
            start_date: 开始日期
            end_date: 结束日期
            year: 年份
            month: 月份
            limit: 分页大小
            offset: 分页偏移
            
        Returns:
            订阅列表
        """
        logger.info(f"Querying raw subscriptions for user: {user_id}")
        
        async with measure_time("database_query"):
            async with AsyncSessionLocal() as session:
                query = select(SubscriptionRecords).where(
                    SubscriptionRecords.user_id == user_id
                )
                
                # 精确查询
                if ind:
                    query = query.where(SubscriptionRecords.ind == ind)
                
                # 日期范围
                elif start_date != "string" and end_date != "string":
                    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
                    query = query.where(SubscriptionRecords.start_date >= start_dt)
                    
                    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
                    query = query.where(SubscriptionRecords.start_date <= end_dt)
                
                # 分页
                elif limit:
                    query = query.order_by(
                        SubscriptionRecords.start_date.desc()
                    ).offset(offset).limit(limit)
                
                # 按月查询
                elif year and month:
                    start_dt = datetime(year, month, 1)
                    _, last_day = calendar.monthrange(year, month)
                    end_dt = datetime(year, month, last_day, 23, 59, 59, 999999)
                    query = query.where(
                        SubscriptionRecords.start_date >= start_dt,
                        SubscriptionRecords.start_date <= end_dt
                    )
                    logger.info(f"Monthly query: {year}-{month:02d}")
                
                # 默认查询当年
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
            return {
                "message": "No records found",
                "data": [],
                "total": 0,
                "status": "success"
            }
        
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
    
    @staticmethod
    async def get_subscription_stats(
        user_id: str,
        year: Optional[int] = None
    ) -> Dict:
        """
        获取订阅统计（仅统计非过期）
        
        Args:
            user_id: 用户 ID
            year: 年份（可选）
            
        Returns:
            统计数据
        """
        logger.info(f"Generating subscription statistics for user: {user_id}")
        
        if not year:
            year = datetime.now().year
        
        # 查询所有订阅
        async with measure_time("database_query"):
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(SubscriptionRecords).where(
                        SubscriptionRecords.user_id == user_id
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
        
        # 解密
        async with measure_time("decrypt_records"):
            subscriptions = await asyncio.gather(
                *[process_record(r, "subscription_records") for r in records]
            )
        
        # 计算实时状态并过滤
        today = datetime.utcnow().date()
        async with measure_time("enrich_and_filter"):
            enriched_subscriptions = await asyncio.gather(
                *[SubscriptionRecordsService._enrich_subscription(s, today) 
                  for s in subscriptions]
            )
            
            active_subscriptions = [
                s for s in enriched_subscriptions 
                if s.get("status_label") in ["active", "upcoming"]
            ]
        
        if not active_subscriptions:
            return {
                "overview": {
                    "total_active": 0,
                    "annual_costs_by_currency": {},
                    "monthly_average_by_currency": {}
                },
                "by_currency": [],
                "by_billing_cycle": []
            }
        
        # 计算统计
        async with measure_time("calculate_stats"):
            stats = await asyncio.to_thread(
                SubscriptionRecordsService._calculate_subscription_stats,
                active_subscriptions
            )
        
        return stats
    
    @staticmethod
    async def update_subscription(
        ind: int,
        user_id: str,
        update_fields: Dict
    ) -> Dict:
        """
        更新订阅记录
        
        Args:
            ind: 记录 ID
            user_id: 用户 ID
            update_fields: 要更新的字段
            
        Returns:
            更新后的记录
        """
        if not update_fields:
            return {"message": "No data to update", "status": "success"}
        
        # 检查是否更新了关键字段
        key_fields = ['buyer_name', 'seller_name', 'plan_name', 'currency', 'amount']
        has_key_field_change = any(field in update_fields for field in key_fields)
        
        # ========== 阶段 1：查询现有数据 ==========
        async with AsyncSessionLocal() as session:
            query_result = await session.execute(
                select(SubscriptionRecords).where(
                    and_(
                        SubscriptionRecords.ind == ind,
                        SubscriptionRecords.user_id == user_id
                    )
                )
            )
            existing_record = query_result.scalar_one_or_none()
            
            if not existing_record:
                return {"error": "No matching record found", "status": "error"}
            
            canonical_id = existing_record.canonical_id
            existing_dict = existing_record.__dict__.copy()
        
        # ========== 阶段 2：数据处理 ==========
        existing_data = await process_record(existing_dict, "subscription_records")
        merged_data = existing_data.copy()
        merged_data.update(update_fields)
        
        # 准备 canonical 更新
        canonical_update = None
        old_normalized_key = None
        
        if has_key_field_change and canonical_id:
            async with AsyncSessionLocal() as session:
                canonical_result = await session.execute(
                    select(CanonicalEntities).where(
                        CanonicalEntities.id == canonical_id
                    )
                )
                canonical = canonical_result.scalar_one_or_none()
            
            if canonical:
                old_normalized_key = canonical.normalized_key
                
                canonical_update = {
                    'canonical_buyer_name': update_fields.get('buyer_name', canonical.canonical_buyer_name),
                    'canonical_seller_name': update_fields.get('seller_name', canonical.canonical_seller_name),
                    'canonical_plan_name': update_fields.get('plan_name', canonical.canonical_plan_name),
                    'canonical_currency': update_fields.get('currency', canonical.canonical_currency),
                    'canonical_amount': Decimal(str(update_fields.get('amount', canonical.canonical_amount)))
                }
                
                new_normalized_key = generate_normalized_key(
                    canonical_update['canonical_buyer_name'],
                    canonical_update['canonical_seller_name'],
                    canonical_update['canonical_plan_name'],
                    canonical_update['canonical_currency'],
                    float(canonical_update['canonical_amount'])
                )
                
                canonical_update['normalized_key'] = new_normalized_key
                canonical_update['updated_at'] = datetime.utcnow()
        
        # 准备订阅记录更新
        update_fields["updated_at"] = datetime.utcnow()
        update_fields["chain_key_bidx"] = SubscriptionRecordsService.calculate_chain_key_bidx(
            user_id, merged_data
        )
        encrypted_update_data = encrypt_data("subscription_records", update_fields)
        encrypted_canonical_update = encrypt_data("canonical_entities", canonical_update)
        
        # ========== 阶段 3：批量写入 ==========
        async with measure_time("database_update"):
            async with AsyncSessionLocal() as session:
                if canonical_update:
                    await session.execute(
                        update(CanonicalEntities)
                        .where(CanonicalEntities.id == canonical_id)
                        .values(**encrypted_canonical_update)
                    )
                
                result = await session.execute(
                    update(SubscriptionRecords)
                    .where(and_(
                        SubscriptionRecords.ind == ind,
                        SubscriptionRecords.user_id == user_id
                    ))
                    .values(**encrypted_update_data)
                    .returning(SubscriptionRecords)
                )
                
                await session.commit()
                updated_records = result.mappings().all()
        
        if not updated_records:
            return {"error": "Update operation failed", "status": "error"}
        
        # ========== 阶段 4：后处理 ==========
        if old_normalized_key:
            asyncio.create_task(
                invalidate_canonical_cache(user_id, old_normalized_key)
            )
            logger.info(f"Updated canonical_entities (id={canonical_id})")
        
        decrypted_result = await asyncio.gather(
            *[process_record(r, "subscription_records") for r in updated_records]
        )
        
        return {
            "message": "Subscription updated successfully",
            "updated_records": len(decrypted_result),
            "data": decrypted_result,
            "status": "success"
        }
    
    @staticmethod
    async def insert_subscription(
        user_id: str,
        insert_fields: Dict
    ) -> Dict:
        """
        插入订阅记录
        
        Args:
            user_id: 用户 ID
            insert_fields: 要插入的字段
            
        Returns:
            插入的记录
        """
        if not insert_fields:
            return {"message": "No data provided", "status": "success"}
        
        # 规范化字段
        normalized = await normalize_subscription_fields(
            {
                'buyer_name': insert_fields.get('buyer_name', ''),
                'seller_name': insert_fields.get('seller_name', ''),
                'plan_name': insert_fields.get('plan_name', ''),
                'currency': insert_fields.get('currency', 'USD'),
                'amount': insert_fields.get('amount', 0)
            },
            user_id
        )
        
        # 使用规范化后的字段
        insert_fields['buyer_name'] = normalized['buyer_name']
        insert_fields['seller_name'] = normalized['seller_name']
        insert_fields['plan_name'] = normalized['plan_name']
        insert_fields['currency'] = normalized['currency']
        insert_fields['amount'] = normalized['amount']
        insert_fields['canonical_id'] = normalized.get('canonical_id')
        
        # 时间戳
        now_utc = datetime.utcnow()
        insert_fields["updated_at"] = now_utc
        if "created_at" not in insert_fields:
            insert_fields["created_at"] = now_utc
        
        # 计算 hash
        insert_fields["chain_key_bidx"] = SubscriptionRecordsService.calculate_chain_key_bidx(
            user_id, insert_fields
        )
        
        # 加密
        encrypted_data = encrypt_data("subscription_records", insert_fields)
        encrypted_data["user_id"] = user_id
        
        # 插入
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
        
        decrypted_result = await asyncio.gather(
            *[process_record(r, "subscription_records") for r in inserted_records]
        )
        
        return {
            "message": "Subscription inserted successfully",
            "affected_records": len(decrypted_result),
            "data": decrypted_result,
            "status": "success"
        }
    
    @staticmethod
    async def delete_subscriptions(
        user_id: str,
        inds: List[int]
    ) -> Dict:
        """
        删除订阅记录
        
        Args:
            user_id: 用户 ID
            inds: 要删除的记录 ID 列表
            
        Returns:
            删除结果
        """
        if not inds:
            return {"error": "inds list cannot be empty", "status": "error"}

        batch_ops = BatchOperations()
        
        async with measure_time("batch_delete"):
            deleted_count = await batch_ops.batch_delete(
                SubscriptionRecords,
                inds,
                key_field='ind'
            )

        return {
            "message": "Records deleted successfully",
            "deleted_count": deleted_count,
            "status": "success"
        }
    
    # ========== 辅助方法 ==========
    
    @staticmethod
    async def _enrich_subscription(record: dict, today: date) -> dict:
        """
        计算订阅实时状态
        
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
    
    @staticmethod
    def _calculate_subscription_stats(subscriptions: List[dict]) -> dict:
        """
        计算订阅统计（同步函数，在线程池中执行）
        
        Args:
            subscriptions: 已经过滤的活跃订阅列表
        """
        total_active = len(subscriptions)
        currency_stats = {}
        by_cycle = {}

        # 按货币统计
        for s in subscriptions:
            currency = s.get("currency", "USD")
            amount = float(s.get("amount") or 0)
            cycle = s.get("billing_cycle", "monthly")
            annual_cost = SubscriptionRecordsService._calculate_annual_cost(amount, cycle)

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
    
    @staticmethod
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