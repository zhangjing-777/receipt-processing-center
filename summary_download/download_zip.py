import os
import re
import uuid
import tempfile
import aiohttp
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client
from zipfile import ZipFile, ZIP_DEFLATED
from io import BytesIO

load_dotenv()

url: str = os.getenv("SUPABASE_URL") or ""
key: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
supabase: Client = create_client(url, key)
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")

logger = logging.getLogger(__name__)


def safe_filename(label: str, url: str) -> str:
    """生成安全的文件名，避免重复扩展名和非法字符"""
    path = url.split("?")[0]  # 去掉 query string
    ext = Path(path).suffix or ".pdf"

    # 如果 label 已经以 .pdf 结尾，就不再拼接
    if label.lower().endswith(ext.lower()):
        file_name = label
    else:
        file_name = f"{label}{ext}"

    # 清理非法字符
    file_name = re.sub(r'[\/:*?"<>| ]', "_", file_name)
    return file_name


async def fetch_file(session, sem, file_url, arcname, retries=3):
    """下载文件，返回 (arcname, content)"""
    async with sem:
        for attempt in range(1, retries + 1):
            try:
                async with session.get(file_url, timeout=30) as resp:
                    if resp.status == 200:
                        content = await resp.read()
                        logger.info(f"Downloaded: {file_url} ({len(content)} bytes)")
                        return arcname, content
                    else:
                        logger.warning(f"Attempt {attempt}: {file_url} - HTTP {resp.status}")
            except Exception as e:
                logger.warning(f"Attempt {attempt}: {file_url} - {e}")

            if attempt < retries:
                await asyncio.sleep(2 * attempt)  # 指数退避

        logger.error(f"All retries failed for {file_url}")
        return arcname, None


async def generate_download_zip(user_id: str, data: dict):
    """生成发票压缩包，上传 Supabase，返回签名下载链接"""
    logger.info("Starting invoice zip generation")

    date_str = datetime.now().strftime("%Y%m%d-%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    zip_name = f"receipt_attachment_{date_str}_{unique_id}.zip"
    upload_path = f"summary/{user_id}/{date_str}/{zip_name}"

    sem = asyncio.Semaphore(10)  # 最多并发 10 个下载
    tasks = []

    async with aiohttp.ClientSession() as session:
        for buyer, date_dict in data.items():
            for invoice_date, category_dict in date_dict.items():
                for category, file_dict in category_dict.items():
                    for file_url, label in file_dict.items():
                        file_name = safe_filename(label, file_url)
                        arcname = f"{buyer}/{invoice_date}/{category}/{file_name}"
                        tasks.append(fetch_file(session, sem, file_url, arcname))

        results = await asyncio.gather(*tasks)

    # 串行写入 zip（保证文件不损坏）
    zip_buffer = BytesIO()
    with ZipFile(zip_buffer, "w", ZIP_DEFLATED) as zipf:
        for arcname, content in results:
            if content:
                zipf.writestr(arcname, content)

    # 上传到 Supabase
    try:
        zip_buffer.seek(0)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmpf:
            tmpf.write(zip_buffer.read())
            tmp_path = tmpf.name

        try:
            supabase.storage.from_(SUPABASE_BUCKET).upload(
                path=upload_path,
                file=tmp_path,  # 上传路径，SDK 会自己打开文件
                file_options={"content-type": "application/zip"}
            )
            logger.info(f"Uploaded zip to Supabase Storage: {upload_path}")
        finally:
            os.remove(tmp_path)

    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise

    # 生成签名下载 URL（24 小时有效）
    try:
        signed_url_result = supabase.storage.from_(SUPABASE_BUCKET).create_signed_url(
            upload_path, expires_in=86400
        )
        download_url = signed_url_result.get("signedURL", upload_path)
        logger.info(f"Generated signed URL: {download_url}, the download_url is only valid for 24 hours.")
    except Exception as e:
        logger.warning(f"Failed to generate signed URL: {e}")
        download_url = upload_path

    logger.info("Zip created and uploaded successfully.")

    return download_url
