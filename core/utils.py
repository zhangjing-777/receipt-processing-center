import re
import json
import unicodedata
import hashlib
from pypinyin import lazy_pinyin
import logging


logger = logging.getLogger(__name__)

def make_safe_storage_path(filename: str, prefix: str = "") -> str:
    logger.info(f"Sanitizing filename: {filename}")
    # 1. 去除不可见字符 + 正规化为 NFC
    filename = unicodedata.normalize("NFKC", filename)

    # 2. 中文转拼音（只保留文件主名，后缀不处理）
    if "." in filename:
        name_part, ext = filename.rsplit(".", 1)
    else:
        name_part, ext = filename, ""

    # 转为拼音（如：'天翔迪晟（深圳）发票' → 'tianxiangdisheng_shenzhen_fapiao'）
    pinyin_name = "_".join(lazy_pinyin(name_part))

    # 3. 保留英文、数字、下划线、短横线和点，移除非法字符
    pinyin_name = re.sub(r"[^\w.-]", "_", pinyin_name)
    ext = re.sub(r"[^\w]", "", ext)

    # 4. 限长 + 防重复 hash
    if len(pinyin_name) > 80:
        hash_suffix = hashlib.md5(filename.encode()).hexdigest()[:8]
        pinyin_name = pinyin_name[:70] + "_" + hash_suffix

    # 5. 组装最终文件名
    final_filename = f"{pinyin_name}.{ext}" if ext else pinyin_name

    # 6. 可选前缀路径（如 '2025-06-23'）
    if prefix:
        result = f"{prefix}/{final_filename}"
    else:
        result = final_filename
    logger.info(f"Sanitized filename result: {result}")
    return result

def clean_and_parse_json(text: str) -> dict:
    logger.info("Cleaning and parsing JSON text.")
    try:
        # 尝试清洗 Markdown 代码块 ```json 或 ``` 包裹的内容
        cleaned = re.sub(r"^```(?:json|python)?\n", "", text.strip(), flags=re.IGNORECASE)
        cleaned = re.sub(r"\n```$", "", cleaned.strip())
        # 加载为 JSON 字典
        result = json.loads(cleaned)
        logger.info("JSON parsed successfully.")
        return result
    except Exception as e:
        logger.exception(f"Failed to clean and parse JSON: {str(e)}")
        raise
