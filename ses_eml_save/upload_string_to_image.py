import os
import uuid
import logging
from datetime import datetime
from supabase import create_client, Client
from playwright.async_api import async_playwright
from core.config import settings


# 设置日志
logger = logging.getLogger(__name__)

supabase: Client = create_client(settings.supabase_url, settings.supabase_service_role_key)


async def render_html_string_to_image_and_upload(html_string: str, user_id:str, filename: str) -> dict:
    logger.info(f"Starting HTML to image conversion for filename: {filename}")
    
    # 生成唯一文件名
    image_file = f"eml_body_{datetime.utcnow().timestamp()}_{str(uuid.uuid4())[:8]}.png"
    logger.info(f"Generated temporary image filename: {image_file}")

    try:
        # 用 Playwright 渲染 HTML 字符串
        logger.info("Launching Playwright browser for HTML rendering")
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            logger.info("Setting HTML content in browser page")
            await page.set_content(html_string)
            logger.info("Taking screenshot of HTML content")
            await page.screenshot(path=image_file, full_page=True)
            await browser.close()
        
        logger.info(f"Screenshot saved to local file: {image_file}")

        # 上传至 Supabase Storage
        storage_path = f"save/{user_id}/{datetime.utcnow().date().isoformat()}/{image_file}"
        logger.info(f"Uploading image to Supabase Storage: {storage_path}")
        
        with open(image_file, "rb") as f:
            supabase.storage.from_(settings.supabase_bucket).upload(storage_path, f, {
                "content-type": "image/png"
            })
        
        logger.info("Image uploaded successfully to Supabase Storage")

        # 获取公开 URL
        #public_url = supabase.storage.from_(settings.supabase_bucket).get_public_url(storage_path).rstrip('?')
        #logger.info(f"Generated public URL: {public_url}")

        # 清理本地临时文件
        os.remove(image_file)
        logger.info(f"Local temporary file removed: {image_file}")
        
        public_urls = {}
        public_urls[filename] = storage_path
        logger.info(f"HTML to image conversion completed successfully for: {filename}")
        return public_urls
        
    except Exception as e:
        logger.exception(f"Failed to convert HTML to image for {filename}: {str(e)}")
        # 确保清理临时文件
        if os.path.exists(image_file):
            try:
                os.remove(image_file)
                logger.info(f"Cleaned up temporary file after error: {image_file}")
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup temporary file {image_file}: {str(cleanup_error)}")
        raise
