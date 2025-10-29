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
        self.model = (
            ReceiptUsageQuotaReceiptEN 
            if table == "receipt_usage_quota_receipt_en" 
            else ReceiptUsageQuotaRequestEN
        )

    async def check_and_reset(self):
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
            allowed = self.used_month < (self.month_limit+self.raw_limit)

            if not allowed:
                remark = "⚠️ You have reached your month usage limit. Please try next period or upgrade your plan for more quota."
                await session.execute(
                    update(self.model)
                    .where(self.model.user_id == self.user_id)
                    .values(remark=remark)
                )
                await session.commit()
                raise ValueError(remark)

            logger.info(f"Quota check - user_id: {self.user_id}, used: {self.used_month}/({self.month_limit}+{self.raw_limit})")

    async def increment_usage(self, success_count: int):
        """
        异步增加使用量
        
        Args:
            success_count: 成功处理的数量
        """
        remain_raw = max(self.raw_limit-success_count, 0)
        new_used = self.used_month - min(self.raw_limit-success_count, 0)
        
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(self.model)
                .where(self.model.user_id == self.user_id)
                .values(used_month=new_used, raw_limit=remain_raw)
            )
            await session.commit()
            
        self.used_month = new_used
        self.raw_limit = remain_raw
        logger.info(f"Quota updated - user_id: {self.user_id}, new usage: {self.used_month}/({self.month_limit}+{self.raw_limit})")
    
    async def get_remaining(self) -> int:
        """
        获取剩余配额
        
        Returns:
            剩余可用次数
        """
        return max(0, self.month_limit+self.raw_limit - self.used_month)
    
    async def get_usage_percentage(self) -> float:
        """
        获取使用百分比
        
        Returns:
            使用百分比 (0-100)
        """
        if self.month_limit+self.raw_limit == 0:
            return 0.0
        return (self.used_month / (self.month_limit+self.raw_limit)) * 100
