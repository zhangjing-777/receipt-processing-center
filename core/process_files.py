import asyncio
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
    """
    异步处理单个文件 (已完全异步化)
    
    Args:
        filename: 文件名
        public_url: 文件存储路径或 URL
        user_id: 用户 ID
        ocr_func: OCR 函数 (必须是异步函数)
        extract_func: 字段提取函数 (必须是异步函数)
        analyze_func: 订阅分析函数 (必须是异步函数)
        
    Returns:
        处理结果字典
    """
    try:
        # OCR 处理 (现在是异步)
        logger.info(f"Starting OCR for {filename}")
        ocr = await ocr_func(public_url)
        logger.info(f"OCR completed for {filename}, length: {len(ocr)}")
        
        # 字段提取和订阅分析并行执行
        logger.info(f"Starting parallel extraction for {filename}")
        fields_task = extract_func(ocr)
        subscription_task = analyze_func(ocr)
        
        fields, subscription_analysis = await asyncio.gather(
            fields_task, 
            subscription_task,
            return_exceptions=True
        )
        
        if isinstance(fields, Exception):
            logger.error(f"Field extraction failed for {filename}: {fields}")
            raise fields
        
        logger.info(f"Extraction completed for {filename}")
        
        # 解析订阅分析结果
        is_subscription = False
        subscription_fields = None
        if not isinstance(subscription_analysis, Exception):
            try:
                from core.utils import clean_and_parse_json
                sub_data = clean_and_parse_json(subscription_analysis)
                is_subscription = sub_data.get("is_subscription", False)
                subscription_fields = sub_data.get("subscription_fields") if is_subscription else None
                
                if is_subscription:
                    logger.info(f"✅ Detected subscription invoice: {filename}")
            except Exception as e:
                logger.warning(f"Failed to parse subscription analysis for {filename}: {e}")
        else:
            logger.warning(f"Subscription analysis failed for {filename}: {subscription_analysis}")
            
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
    并行处理多个文件，限制并发数 (完全异步化)
    
    Args:
        public_urls: {filename: storage_path}
        user_id: 用户 ID
        ocr_func: OCR 函数 (异步)
        extract_func: 字段提取函数 (异步)
        analyze_func: 订阅分析函数 (异步)
        max_concurrent: 最大并发数
        
    Returns:
        (成功结果列表, 成功文件名列表, 失败文件名列表, 订阅文件名列表)
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_with_limit(filename, url):
        async with semaphore:
            logger.info(f"Processing file: {filename}")
            result = await process_single_file_async(
                filename, url, user_id,
                ocr_func, extract_func, analyze_func
            )
            logger.info(f"Completed processing: {filename} - Status: {result['status']}")
            return result
    
    # 创建所有任务
    logger.info(f"Starting parallel processing for {len(public_urls)} file(s) with max_concurrent={max_concurrent}")
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
    
    logger.info(
        f"Processing complete - Success: {len(success_files)}, "
        f"Failed: {len(failed_files)}, Subscriptions: {len(subscription_files)}"
    )
    
    return successes, success_files, failed_files, subscription_files
