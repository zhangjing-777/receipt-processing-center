import uuid
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Any
from core.utils import clean_and_parse_json
from core.canonicalization import normalize_subscription_fields


logger = logging.getLogger(__name__)


RECORD_ID = str(uuid.uuid4())

class ReceiptDataPreparer:
    def __init__(self, user_id, fields: str, raw_attachments: Dict[str, Any], public_url: str, ocr: str):
        self.fields = fields
        self.raw_attachments = raw_attachments
        self.public_url = public_url
        self.ocr = ocr

        # 用户名 = 邮箱前缀
        self.user_id = user_id

        # 解析字段
        self.items = self.parse_fields()

    def parse_fields(self) -> Dict[str, Any]:
        """清洗字段并返回 dict"""
        logger.info("Parsing and cleaning fields for receipt data.")
        try:
            items = clean_and_parse_json(self.fields)
        except Exception as e:
            logger.exception(f"Failed to parse fields: {str(e)}")
            raise ValueError(f"Failed to parse fields: {e}")

        # 生成 hash_id，防重复
        hash_input = "|".join([
            str(self.user_id),
            str(items.get("invoice_total", "")),
            str(items.get("buyer", "")),
            str(items.get("seller", "")),
            str(items.get("invoice_date", "")),
            str(items.get("invoice_number", ""))
        ])

        items["hash_id"] = hashlib.md5(hash_input.encode()).hexdigest()
        logger.info(f"Generated hash_id for receipt: {items['hash_id']}")
        return items

    def build_receipt_data(self) -> Dict[str, Any]:
        logger.info("Building receipt data dictionary.")
        try:
            data = {
                "id": RECORD_ID,
                "user_id": self.user_id,
                "file_url": self.public_url,
                "original_info": self.raw_attachments.get("body", ""),
                "ocr": self.ocr,
                "create_time": datetime.utcnow().isoformat(),
                **self.items  # 合并提取字段
            }
            logger.info("Receipt data built successfully.")
            return data
        except Exception as e:
            logger.exception(f"Failed to build receipt data: {str(e)}")
            raise

    def build_eml_data(self, s3_eml_url: str) -> Dict[str, Any]:
        logger.info("Building EML data dictionary.")
        try:
            data = {
                "id": RECORD_ID,
                "user_id": self.user_id,
                "from_email": self.raw_attachments.get("from_email", ""),
                "to_email": self.raw_attachments.get("to_email", ""),
                "s3_eml_url": s3_eml_url,
                "buyer": self.items.get("buyer", ""),
                "seller": self.items.get("seller", ""),
                "invoice_date": self.items.get("invoice_date", ""),
                "create_time": datetime.utcnow().isoformat()
            }
            logger.info("EML data built successfully.")
            return data
        except Exception as e:
            logger.exception(f"Failed to build EML data: {str(e)}")
            raise


class SubscriptDataPreparer:
    def __init__(self, fields: str, user_id: str, source: str):
        self.user_id = user_id
        self.source = source
        self.fields = clean_and_parse_json(fields)

    async def build_subscript_data(self) -> Dict[str, Any]:
        logger.info("Building subscript data dictionary.")
        try:
            # 1. 规范化字段
            normalized = await normalize_subscription_fields(
                {
                    'buyer_name': self.fields.get("buyer_name", ""),
                    'seller_name': self.fields.get("seller_name", ""),
                    'plan_name': self.fields.get("plan_name", ""),
                    'currency': self.fields.get("currency", "USD"),
                    'amount': self.fields.get("amount", 0)
                },
                self.user_id
            )
            
            # 2. 生成 hash_id（使用规范化后的字段）
            hash_input = "|".join([
                str(self.user_id),
                str(normalized['buyer_name']),
                str(normalized['seller_name']),
                str(normalized['plan_name']),
                str(normalized['currency']),
                str(normalized['amount'])
            ])
            chain_key_bidx = hashlib.md5(hash_input.encode()).hexdigest()

            # 3. 构建数据
            data = {
                "id": RECORD_ID,
                "user_id": self.user_id,
                "buyer_name": normalized['buyer_name'],
                "seller_name": normalized['seller_name'],
                "plan_name": normalized['plan_name'],
                "billing_cycle": self.fields.get("billing_cycle", "monthly"),
                "amount": normalized['amount'],
                "currency": normalized['currency'],
                "start_date": self.fields.get("start_date"),
                "next_renewal_date": self.fields.get("next_renewal_date"),
                "end_date": self.fields.get("end_date"),
                "source": self.source,
                "note": self.fields.get("note"),
                "chain_key_bidx": chain_key_bidx,
                "canonical_id": normalized.get('canonical_id'),
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat()
            }
            
            logger.info(f"Subscript data built successfully: {data}")
            return data
        except Exception as e:
            logger.exception(f"Failed to build subscript data: {str(e)}")
            raise
