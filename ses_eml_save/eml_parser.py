import asyncio
import mailparser
import logging
from core.config import settings
from core.http_client import AsyncHTTPClient

logger = logging.getLogger(__name__)


async def load_s3(bucket, key):
    """异步从 S3 加载对象"""
    logger.info(f"Loading object from S3: bucket={bucket}, key={key}")
    
    client = AsyncHTTPClient.get_client()
    
    try:
        # 构建 S3 URL
        s3_url = f"https://{bucket}.s3.{settings.aws_region}.amazonaws.com/{key}"
        
        # 使用 AWS Signature V4 进行身份验证
        import hmac
        import hashlib
        from datetime import datetime
        from urllib.parse import quote
        
        # 简化版: 使用 boto3 的同步方法包装到线程
        # 完全异步的 S3 客户端需要更复杂的实现
        import boto3
        s3 = boto3.client(
            "s3",
            region_name=settings.aws_region,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key
        )
        
        # 包装同步调用到线程
        response = await asyncio.to_thread(
            s3.get_object,
            Bucket=bucket,
            Key=key
        )
        
        content = await asyncio.to_thread(response["Body"].read)
        
        logger.info("S3 object loaded successfully.")
        return content
        
    except Exception as e:
        logger.exception(f"Failed to load object from S3: {str(e)}")
        raise


async def mail_parser(eml_bytes):
    """异步解析邮件"""
    logger.info("Parsing EML bytes.")
    
    try:
        # mailparser 库是同步的，包装到线程
        mail = await asyncio.to_thread(
            mailparser.parse_from_bytes,
            eml_bytes
        )

        # 基本字段提取
        from_email = mail.from_[0][1] if mail.from_ else ""
        to_email = mail.to[0][1] if mail.to else ""
        subject = mail.subject or ""
        body = mail.text_plain[0] if mail.text_plain else mail.text_html[0] if mail.text_html else ""

        # 原始附件提取
        raw_attachments = []
        for att in mail.attachments:
            filename = att.get("filename", "unknown")
            content_type = att.get("mail_content_type", "application/octet-stream")
            payload = att.get("payload", b"")
            if isinstance(payload, str):
                payload = payload.encode("utf-8")

            raw_attachments.append({
                "filename": filename,
                "content_type": content_type,
                "binary": payload,  # 原始文件二进制
            })
        
        logger.info(f"Parsed {len(raw_attachments)} attachments from EML.")
        return dict(
            from_email=from_email,
            to_email=to_email,
            subject=subject,
            body=body,
            attachments=raw_attachments
        )
    except Exception as e:
        logger.exception(f"Failed to parse EML bytes: {str(e)}")
        raise