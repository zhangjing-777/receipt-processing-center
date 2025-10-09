import asyncio
import aiohttp
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

async def process_single_file_async(
    filename: str,
    public_url: str,
    user_id: str,
    ocr_func,
    extract_func,
    analyze_func
) -> Dict:
    """异步处理单个文件"""
    try:
        # OCR 处理
        ocr = await asyncio.to_thread(ocr_func, public_url)
        
        # 字段提取（并行）
        fields_task = asyncio.to_thread(extract_func, ocr)
        subscription_task = asyncio.to_thread(analyze_func, ocr)
        
        fields, subscription_analysis = await asyncio.gather(
            fields_task, 
            subscription_task,
            return_exceptions=True
        )
        
        if isinstance(fields, Exception):
            raise fields
        
        # 解析订阅分析结果
        is_subscription = False
        subscription_fields = None
        if not isinstance(subscription_analysis, Exception):
            try:
                from core.utils import clean_and_parse_json
                sub_data = clean_and_parse_json(subscription_analysis)
                is_subscription = sub_data.get("is_subscription", False)
                subscription_fields = sub_data.get("subscription_fields") if is_subscription else None
            except Exception as e:
                logger.warning(f"Failed to parse subscription analysis: {e}")
            
        return {
            "status": "success",
            "filename": filename,
            "ocr": ocr,
            "fields": fields,
            "is_subscription": is_subscription,
            "subscription_fields": subscription_fields,
            "public_url": public_url
        }
    except Exception as e:
        logger.exception(f"Failed to process {filename}: {str(e)}")
        return {
            "status": "error",
            "filename": filename,
            "error": str(e)
        }


async def process_files_parallel(
    public_urls: Dict[str, str],
    user_id: str,
    ocr_func,
    extract_func,
    analyze_func,
    max_concurrent: int = 5
) -> tuple[List[Dict], List[str], List[str], List[str]]:
    """
    并行处理多个文件，限制并发数
    
    Args:
        public_urls: {filename: storage_path}
        user_id: 用户ID
        ocr_func: OCR 函数
        extract_func: 字段提取函数
        analyze_func: 订阅分析函数
        max_concurrent: 最大并发数
        
    Returns:
        (成功结果列表, 成功文件名列表, 失败文件名列表, 订阅文件名列表)
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_with_limit(filename, url):
        async with semaphore:
            return await process_single_file_async(
                filename, url, user_id,
                ocr_func, extract_func, analyze_func
            )
    
    # 创建所有任务
    tasks = [
        process_with_limit(filename, url)
        for filename, url in public_urls.items()
    ]
    
    # 并行执行
    results = await asyncio.gather(*tasks)
    
    # 分类结果
    successes = []
    success_files = []
    failed_files = []
    subscription_files = []
    
    for result in results:
        if result["status"] == "success":
            successes.append(result)
            success_files.append(result["filename"])
            # 检查是否为订阅文件
            if result.get("is_subscription"):
                subscription_files.append(result["filename"])
        else:
            failed_files.append(f"{result['filename']} - {result.get('error', 'Unknown error')}")
    
    return successes, success_files, failed_files, subscription_files