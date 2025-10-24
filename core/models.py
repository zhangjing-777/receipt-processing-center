from sqlalchemy import Column, Text, DateTime as SQLDateTime, BigInteger, Numeric, TypeDecorator, Date as SQLDate, Integer, Boolean
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime, date
import uuid
from core.database import Base


# 🔥 自定义 Date 类型，自动转换字符串
class AutoConvertDate(TypeDecorator):
    """自动将字符串转换为 date 对象的类型"""
    impl = SQLDate
    cache_ok = True
    
    def process_bind_param(self, value, dialect):
        """插入数据库前处理（Python → DB）"""
        if value is None:
            return None
        if isinstance(value, str):
            try:
                return datetime.strptime(value, "%Y-%m-%d").date()
            except ValueError:
                return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return None
    
    def process_result_value(self, value, dialect):
        """从数据库读取后处理（DB → Python）"""
        return value


# 🔥 自定义 DateTime 类型，自动转换字符串
class AutoConvertDateTime(TypeDecorator):
    """自动将字符串转换为 datetime 对象的类型"""
    impl = SQLDateTime(timezone=True)
    cache_ok = True
    
    def process_bind_param(self, value, dialect):
        """插入数据库前处理（Python → DB）"""
        if value is None:
            return None
        if isinstance(value, str):
            try:
                # 处理 ISO 格式：2025-10-10T18:25:47.233500
                if 'T' in value:
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                else:
                    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
        if isinstance(value, datetime):
            return value
        return None
    
    def process_result_value(self, value, dialect):
        """从数据库读取后处理（DB → Python）"""
        return value


class ReceiptItemsEN(Base):
    __tablename__ = "receipt_items_en"
    
    ind = Column(BigInteger, primary_key=True, autoincrement=True)
    id = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
    user_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    buyer = Column(Text)
    seller = Column(Text)
    invoice_date = Column(AutoConvertDate, index=True)
    category = Column(Text)
    invoice_total = Column(Numeric(10, 2))
    currency = Column(Text)
    invoice_number = Column(Text)
    address = Column(Text)
    file_url = Column(Text)
    original_info = Column(Text)
    ocr = Column(Text)
    hash_id = Column(Text, unique=True, index=True)
    create_time = Column(AutoConvertDateTime, default=datetime.utcnow, index=True)  # 🔥 使用自定义类型

class SesEmlInfoEN(Base):
    __tablename__ = "ses_eml_info_en"
    
    ind = Column(BigInteger, primary_key=True, autoincrement=True)
    id = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
    user_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    from_email = Column(Text)
    to_email = Column(Text)
    s3_eml_url = Column(Text)
    buyer = Column(Text)
    seller = Column(Text)
    invoice_date = Column(AutoConvertDate)
    create_time = Column(AutoConvertDateTime, default=datetime.utcnow, index=True)  # 🔥 使用自定义类型

class SubscriptionRecords(Base):
    __tablename__ = "subscription_records"
    
    ind = Column(BigInteger, primary_key=True, autoincrement=True)
    id = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
    user_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    buyer_name = Column(Text)
    seller_name = Column(Text)
    plan_name = Column(Text)
    billing_cycle = Column(Text)
    amount = Column(Numeric(10, 2))
    currency = Column(Text)
    start_date = Column(AutoConvertDate, index=True)
    next_renewal_date = Column(AutoConvertDate)
    end_date = Column(AutoConvertDate)
    source = Column(Text)
    note = Column(Text)
    chain_key_bidx = Column(Text, index=True)  # 订阅链哈希索引
    canonical_id = Column(Integer, index=True) # 关联canonical_entities表id
    created_at = Column(AutoConvertDateTime, default=datetime.utcnow)  # 🔥 使用自定义类型
    updated_at = Column(AutoConvertDateTime, default=datetime.utcnow, onupdate=datetime.utcnow)  # 🔥 使用自定义类型

class ReceiptSummaryZipEN(Base):
    __tablename__ = "receipt_summary_zip_en"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at = Column(AutoConvertDateTime, default=datetime.utcnow, index=True)  # 🔥 使用自定义类型
    user_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    summary_content = Column(Text)
    title = Column(Text)
    download_url = Column(Text)

class ReceiptItemsENUploadResult(Base):
    __tablename__ = "receipt_items_en_upload_result"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    upload_result = Column(Text)
    created_at = Column(AutoConvertDateTime, default=datetime.utcnow, index=True)  # 🔥 使用自定义类型

class ReceiptUsageQuotaReceiptEN(Base):
    __tablename__ = "receipt_usage_quota_receipt_en"
    
    user_id = Column(UUID(as_uuid=True), primary_key=True)
    created_at = Column(AutoConvertDateTime, default=datetime.utcnow)  # 🔥 使用自定义类型
    month_limit = Column(Integer)
    used_month = Column(Integer, default=0)
    last_reset_date = Column(AutoConvertDate)
    email = Column(Text)
    remark = Column(Text)

class ReceiptUsageQuotaRequestEN(Base):
    __tablename__ = "receipt_usage_quota_request_en"
    
    user_id = Column(UUID(as_uuid=True), primary_key=True)
    created_at = Column(AutoConvertDateTime, default=datetime.utcnow)  # 🔥 使用自定义类型
    month_limit = Column(Integer)
    used_month = Column(Integer, default=0)
    last_reset_date = Column(AutoConvertDate)
    email = Column(Text)
    remark = Column(Text)

class CanonicalEntities(Base):
    __tablename__ = "canonical_entities"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    
    # 规范化字段
    canonical_buyer_name = Column(Text, nullable=False)
    canonical_seller_name = Column(Text, nullable=False)
    canonical_plan_name = Column(Text, nullable=False)
    canonical_currency = Column(Text, nullable=False)
    canonical_amount = Column(Numeric(10, 2), nullable=False)
    
    # 匹配键
    normalized_key = Column(Text, nullable=False)
    
    # 统计
    match_count = Column(Integer, default=1)
    last_matched_at = Column(AutoConvertDateTime)
    
    # 管理
    is_active = Column(Boolean, default=True)  # SQLite 兼容，1=True, 0=False
    notes = Column(Text)
    
    created_at = Column(AutoConvertDateTime, default=datetime.utcnow)
    updated_at = Column(AutoConvertDateTime, default=datetime.utcnow, onupdate=datetime.utcnow)