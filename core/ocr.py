import os
import base64
import asyncio
import logging
from supabase import create_client, Client
from core.http_client import AsyncHTTPClient
from core.config import settings

logger = logging.getLogger(__name__)


MODEL = settings.model
MODEL_FREE = settings.model_free
OPENROUTER_URL = settings.supabase_url
SUPABASE_BUCKET = settings.supabase_bucket
supabase: Client = create_client(OPENROUTER_URL, settings.supabase_service_role_key)

HEADERS = {
    "Authorization": f"Bearer {settings.openrouter_api_key}",
    "Content-Type": "application/json",
    # OpenRouter 推荐（用于统计 & 风控，不加也能跑，但不建议省）
    "HTTP-Referer": settings.openrouter_site,
    "X-Title": settings.openrouter_app_name,
}


async def call_openrouter(model_name: str, messages) -> str:
    payload = {
        "model": model_name,
        "messages": messages,
    }

    client = AsyncHTTPClient.get_client()
    resp = await client.post(
        OPENROUTER_URL,
        headers=HEADERS,
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()

    data = resp.json()
    if "choices" not in data or not data["choices"]:
        raise ValueError("OpenRouter returned no choices")

    if "usage" in data:
        usage = data["usage"]
        logger.info(
            f"[{model_name}] OCR tokens - "
            f"prompt: {usage.get('prompt_tokens')}, "
            f"completion: {usage.get('completion_tokens')}, "
            f"total: {usage.get('total_tokens')}"
        )

    content = data["choices"][0]["message"].get("content")
    if not content or not isinstance(content, str):
        raise ValueError("Invalid image OCR response content")

    return content

async def openrouter_image_ocr(file_url):
    """异步图片 OCR"""
    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "What's in this image?"
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": file_url
                    }
                }
            ]
        }
    ]
    
    try:
        logger.info(f"Trying image OCR with MODEL_FREE: {MODEL_FREE}")
        return await call_openrouter(MODEL_FREE, messages)

    except Exception as free_err:
        logger.warning(
            f"MODEL_FREE failed ({MODEL_FREE}), fallback to MODEL ({MODEL}). "
            f"Error: {free_err}"
        )

        try:
            return await call_openrouter(MODEL, messages)

        except Exception as paid_err:
            logger.exception(
                f"Both MODEL_FREE and MODEL failed for image OCR. "
                f"Error: {paid_err}"
            )
            raise
 


async def openrouter_pdf_ocr(file_url):
    """异步 PDF OCR"""
    logger.info(f"Starting PDF OCR for: {file_url}")
    
    client = AsyncHTTPClient.get_client()

    response = await client.get(file_url)
    response.raise_for_status()
    base64_pdf = base64.b64encode(response.content).decode('utf-8')
    data_url = f"data:application/pdf;base64,{base64_pdf}"
        
    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "What are the main points in this document?"
                },
                {
                    "type": "file",
                    "file": {
                        "filename": "invoice.pdf",
                        "file_data": data_url
                    }
                },
            ]
        }
    ]
    plugins = [
        {
            "id": "file-parser",
            "pdf": {
                "engine": "pdf-text"  
            }
        }
    ]

    async def call_openrouter(model_name: str) -> str:
        payload = {
            "model": model_name,
            "messages": messages,
            "plugins": plugins,
        }

        resp = await client.post(
            OPENROUTER_URL,
            headers=HEADERS,
            json=payload,
            timeout=90,
        )
        resp.raise_for_status()
        data = resp.json()

        if "choices" not in data or not data["choices"]:
            raise ValueError("OpenRouter returned no choices")

        content = data["choices"][0]["message"].get("content")
        if not content or not isinstance(content, str):
            raise ValueError("Invalid PDF OCR response content")

        if "usage" in data:
            usage = data["usage"]
            logger.info(
                f"[{model_name}] PDF OCR tokens - "
                f"prompt: {usage.get('prompt_tokens')}, "
                f"completion: {usage.get('completion_tokens')}, "
                f"total: {usage.get('total_tokens')}"
            )

        return content
    
    try:
        logger.info(f"Trying PDF OCR with MODEL_FREE: {MODEL_FREE}")
        return await call_openrouter(MODEL_FREE)

    except Exception as free_err:
        logger.warning(
            f"MODEL_FREE failed ({MODEL_FREE}), fallback to MODEL ({MODEL}). "
            f"Error: {free_err}"
        )

        try:
            return await call_openrouter(MODEL)

        except Exception as paid_err:
            logger.exception(
                f"Both MODEL_FREE and MODEL failed for PDF OCR. "
                f"Error: {paid_err}"
            )
            raise


async def ocr_pdf_from_storage(storage_path):
    """从 Supabase 存储下载 PDF 进行 OCR (异步)"""
    logger.info(f"Downloading PDF from storage: {storage_path}")

    file_content = await asyncio.to_thread(
        supabase.storage.from_(SUPABASE_BUCKET).download,
        storage_path
    )
    
    base64_pdf = base64.b64encode(file_content).decode('utf-8')
    data_url = f"data:application/pdf;base64,{base64_pdf}"
    
    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "What are the main points in this document?"
                },
                {
                    "type": "file",
                    "file": {
                        "filename": "invoice.pdf",
                        "file_data": data_url
                    }
                },
            ]
        }
    ]
    plugins = [
        {
            "id": "file-parser",
            "pdf": {
                "engine": "pdf-text"  
            }
        }
    ]

    async def call_openrouter(model_name: str) -> str:
        payload = {
            "model": model_name,
            "messages": messages,
            "plugins": plugins,
        }

        client = AsyncHTTPClient.get_client()
        resp = await client.post(
            OPENROUTER_URL,
            headers=HEADERS,
            json=payload,
            timeout=90,
        )
        resp.raise_for_status()

        data = resp.json()
        if "choices" not in data or not data["choices"]:
            raise ValueError("OpenRouter returned no choices")

        content = data["choices"][0]["message"].get("content")
        if not content or not isinstance(content, str):
            raise ValueError("Invalid PDF OCR response content")

        if "usage" in data:
            usage = data["usage"]
            logger.info(
                f"[{model_name}] PDF OCR tokens - "
                f"prompt: {usage.get('prompt_tokens')}, "
                f"completion: {usage.get('completion_tokens')}, "
                f"total: {usage.get('total_tokens')}"
            )

        return content

    try:
        logger.info(f"Trying PDF OCR with MODEL_FREE: {MODEL_FREE}")
        return await call_openrouter(MODEL_FREE)

    except Exception as free_err:
        logger.warning(
            f"MODEL_FREE failed ({MODEL_FREE}), fallback to MODEL ({MODEL}). "
            f"Error: {free_err}"
        )

        try:
            return await call_openrouter(MODEL)

        except Exception as paid_err:
            logger.exception(
                f"Both MODEL_FREE and MODEL failed for storage PDF OCR. "
                f"Error: {paid_err}"
            )
            raise


async def ocr_image_from_storage(storage_path):
    """从 Supabase 存储下载图片进行 OCR (异步)"""
    logger.info(f"Downloading image from storage: {storage_path}")

    # 使用 Supabase client 下载文件 (临时同步方案)
    file_content = await asyncio.to_thread(
        supabase.storage.from_(SUPABASE_BUCKET).download,
        storage_path
    )
    
    base64_image = base64.b64encode(file_content).decode('utf-8')
    
    # 根据文件扩展名判断 content-type
    content_type = "image/jpeg"  # 默认
    if storage_path.lower().endswith('.png'):
        content_type = "image/png"
    elif storage_path.lower().endswith(('.jpg', '.jpeg')):
        content_type = "image/jpeg"
    
    data_url = f"data:{content_type};base64,{base64_image}"
    
    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "What's in this image?"
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": data_url
                    }
                }
            ]
        }
    ]


    # free → paid fallback
    try:
        logger.info(f"Trying image OCR with MODEL_FREE: {MODEL_FREE}")
        return await call_openrouter(MODEL_FREE, messages)

    except Exception as free_err:
        logger.warning(
            f"MODEL_FREE failed ({MODEL_FREE}), fallback to MODEL ({MODEL}). "
            f"Error: {free_err}"
        )

        try:
            return await call_openrouter(MODEL, messages)

        except Exception as paid_err:
            logger.exception(
                f"Both MODEL_FREE and MODEL failed for storage image OCR. "
                f"Error: {paid_err}"
            )
            raise

async def ocr_attachment(file_path_or_url: str) -> str:
    """异步 OCR 入口函数"""
    if not file_path_or_url or not file_path_or_url.strip():
        raise ValueError(f"Empty file path for OCR: {file_path_or_url}")
    
    logger.info(f"Starting OCR for attachment: {file_path_or_url}")
    try:
        # 判断是存储路径还是完整 URL
        if file_path_or_url.startswith("users/") or (not file_path_or_url.startswith("http")):
            # 是存储路径，需要从 Supabase 下载
            logger.info(f"Processing storage path: {file_path_or_url}")
            if file_path_or_url.endswith("pdf"):
                return await ocr_pdf_from_storage(file_path_or_url)
            else:
                return await ocr_image_from_storage(file_path_or_url)
        else:
            # 是完整 URL，使用原有逻辑
            logger.info(f"Processing URL: {file_path_or_url}")
            if file_path_or_url.endswith("pdf"):
                return await openrouter_pdf_ocr(file_path_or_url)
            else:
                return await openrouter_image_ocr(file_path_or_url)
    except Exception as e:
        logger.error(f"OCR failed for {file_path_or_url}: {str(e)}")
        raise
