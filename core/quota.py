import logging
from datetime import date
from sqlalchemy import select, update
from core.database import AsyncSessionLocal
from core.models import ReceiptUsageQuotaReceiptEN, ReceiptUsageQuotaRequestEN

logger = logging.getLogger(__name__)


class QuotaManager:
    """配额管理器 (完全异步)"""

    def __init__(self, user_id: str, table: str = "receipt_usage_quota_receipt_en"):
        self.user_id = user_id
        self.table = table
        self.used_month = 0
        self.month_limit = 0
        self.raw_limit = 0
        self.annual_limit = 0   
        self.used_annual = 0    
        self.model = (
            ReceiptUsageQuotaReceiptEN 
            if table == "receipt_usage_quota_receipt_en" 
            else ReceiptUsageQuotaRequestEN
        )
    

    async def check_and_reset(self, files_length: int):
        """
        异步检查并重置配额
        
        Raises:
            ValueError: 如果配额不存在或已达上限
        """
        async with AsyncSessionLocal() as session:
            # 查询配额
            result = await session.execute(
                select(self.model).where(self.model.user_id == self.user_id)
            )
            quota_data = result.scalar_one_or_none()
            
            if not quota_data:
                raise ValueError(f"Quota record not found for user_id={self.user_id}")

            # 检查是否需要重置
            today = date.today()
            today_str = today.isoformat()  # "YYYY-MM-DD"
            current_month = today_str[:7]  # "YYYY-MM"
            
            # 处理 last_reset_date（可能是 date 对象或字符串）
            last_reset_date = quota_data.last_reset_date
            if isinstance(last_reset_date, date):
                last_reset_month = last_reset_date.isoformat()[:7]
            elif isinstance(last_reset_date, str):
                last_reset_month = last_reset_date[:7]
            else:
                last_reset_month = ""
            
            needs_reset = current_month != last_reset_month

            # 注意：月度重置只清 used_month，不动 used_annual
            # used_annual 只在年度订阅到期时由 account-management-center 的定时任务清零
            if needs_reset:
                self.used_month = 0
                await session.execute(
                    update(self.model)
                    .where(self.model.user_id == self.user_id)
                    .values(used_month=0, last_reset_date=today)
                )
                await session.commit()
                logger.info(f"Quota reset for user_id: {self.user_id}")
            else:
                self.used_month = quota_data.used_month

            self.month_limit = quota_data.month_limit 
            self.raw_limit = quota_data.raw_limit
            self.annual_limit = quota_data.annual_limit or 0
            self.used_annual = quota_data.used_annual or 0

            # 总可用量 = 永久池raw_limit + 月度剩余 + 年度剩余
            month_remaining = max(self.month_limit - self.used_month, 0) 
            annual_remaining = max(self.annual_limit - self.used_annual, 0)
            total_capacity = self.raw_limit + month_remaining + annual_remaining
            db_allowed = (total_capacity > 0)

            if not db_allowed:
                remark = "⚠️ You have reached your month usage limit. Please try next period or upgrade your plan for more quota."
                await session.execute(
                    update(self.model)
                    .where(self.model.user_id == self.user_id)
                    .values(remark=remark)
                )
                await session.commit()
                raise ValueError(remark)

            use_allowed = total_capacity < files_length

            if use_allowed:
                remark = f"⚠️The number of files awaiting processing exceeds your available quota. You can currently process up to {total_capacity} invoices. Please re-upload no more than {total_capacity} invoices."
                await session.execute(
                    update(self.model)
                    .where(self.model.user_id == self.user_id)
                    .values(remark=remark)
                )
                await session.commit()
                raise ValueError(remark)
            logger.info(
                f"Quota check - user_id: {self.user_id}, used: month{self.used_month}+annual{self.used_annual}/"
                f"(raw:{self.raw_limit}+month:{self.month_limit}+annual_left:{self.annual_limit})"
            )
            
    async def increment_usage(self, success_count: int):
        """
        异步增加使用量
        扣减顺序：raw_limit(永久池) → month_limit(月度) → annual_limit(年度)
        
        Args:
            success_count: 成功处理的数量
        """
        remaining = self.raw_limit-success_count
        remain_raw = max(remaining, 0)
        new_used_month = self.used_month - min(remaining, 0)
        new_used_annual = self.used_annual - min(self.month_limit - new_used_month, 0)
        new_used_month = min(new_used_month, self.month_limit)

        async with AsyncSessionLocal() as session:
            await session.execute(
                update(self.model)
                .where(self.model.user_id == self.user_id)
                .values(used_month=new_used_month, raw_limit=remain_raw, used_annual=new_used_annual)
            )
            await session.commit()
            
        self.used_month = new_used_month
        self.raw_limit = remain_raw
        self.used_annual = new_used_annual
        logger.info(f"Quota updated - user_id: {self.user_id}, new usage: {self.used_month}+{self.used_annual}/({self.month_limit}+{self.raw_limit}+{self.annual_limit})")
    
    async def get_remaining(self) -> int:
        """
        获取剩余配额
        
        Returns:
            剩余可用次数
        """
        return max(0, self.month_limit+self.raw_limit+self.annual_limit - self.used_month - self.used_annual)
    
    async def get_usage_percentage(self) -> float:
        """
        获取使用百分比
        
        Returns:
            使用百分比 (0-100)
        """
        if self.month_limit+self.raw_limit+self.annual_limit == 0:
            return 0.0
        return ((self.used_month+self.used_annual) / (self.month_limit+self.raw_limit+self.annual_limit)) * 100
