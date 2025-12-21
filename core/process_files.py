import asyncio
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)


async def process_single_file_async(
    filename: str,
    public_url: str,
    ocr_func,
    extract_func
) -> Dict:
    """
    异步处理单个文件 (已完全异步化)
    
    Args:
        filename: 文件名
        public_url: 文件存储路径或 URL
        ocr_func: OCR 函数 (必须是异步函数)
        extract_func: 字段提取函数 (必须是异步函数)
        
    Returns:
        处理结果字典
    """
    try:
        # OCR 处理 (现在是异步)
        logger.info(f"Starting OCR for {filename}")
        ocr = await ocr_func(public_url)
        logger.info(f"OCR completed for {filename}, length: {len(ocr)}")
        
        # 字段提取
        logger.info(f"Starting field extraction for {filename}")
        fields = await extract_func(ocr)
        
        logger.info(f"Extraction completed for {filename}")
            
        return {
            "status": "success",
            "filename": filename,
            "ocr": ocr,
            "fields": fields,
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
    ocr_func,
    extract_func,
    max_concurrent: int = 5
) -> tuple[List[Dict], List[str], List[str]]:
    """
    并行处理多个文件，限制并发数 (完全异步化)
    
    Args:
        public_urls: {filename: storage_path}
        user_id: 用户 ID
        ocr_func: OCR 函数 (异步)
        extract_func: 字段提取函数 (异步)
        analyze_func: 订阅分析函数 (保留参数但不再使用)
        max_concurrent: 最大并发数
        
    Returns:
        (成功结果列表, 成功文件名列表, 失败文件名列表)
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_with_limit(filename, url):
        async with semaphore:
            logger.info(f"Processing file: {filename}")
            result = await process_single_file_async(
                filename, url, ocr_func, extract_func
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
    
    for result in results:
        if result["status"] == "success":
            successes.append(result)
            success_files.append(result["filename"])
        else:
            failed_files.append(f"{result['filename']} - {result.get('error', 'Unknown error')}")
    
    logger.info(
        f"Processing complete - Success: {len(success_files)}, "
        f"Failed: {len(failed_files)}"
    )
    
    return successes, success_files, failed_files
