import re
import json
import hashlib
import logging
from datetime import datetime
from typing import Dict, Optional
from decimal import Decimal
from sqlalchemy import select, insert, update, func
from core.database import AsyncSessionLocal
from core.models import CanonicalEntities
from core.redis_client import redis_client
from core.encryption import encrypt_value, decrypt_value

logger = logging.getLogger(__name__)

# 缓存配置
CACHE_TTL = 63072000  # 2年
FUZZY_THRESHOLD = 0.85  # 模糊匹配阈值


def generate_normalized_key(
    buyer_name: str,
    seller_name: str,
    plan_name: str,
    currency: str,
    amount: float
) -> str:
    """
    生成规范化匹配键
    
    规则：
    - 去括号及其内容
    - 转小写
    - 去除所有非字母数字字符
    - 金额保留2位小数
    """
    # 去括号
    buyer = re.sub(r'\([^)]*\)', '', str(buyer_name))
    seller = re.sub(r'\([^)]*\)', '', str(seller_name))
    plan = re.sub(r'\([^)]*\)', '', str(plan_name))
    
    # 清洗
    buyer = re.sub(r'[^a-z0-9]', '', buyer.lower().strip())
    seller = re.sub(r'[^a-z0-9]', '', seller.lower().strip())
    plan = re.sub(r'[^a-z0-9]', '', plan.lower().strip())
    currency = str(currency).upper().strip()
    
    # 金额保留2位小数
    amount_str = f"{float(amount):.2f}"

    hash_input = "|".join([buyer, seller, plan, currency, amount_str])
    return hashlib.md5(hash_input.encode()).hexdigest()
    

async def normalize_subscription_fields(
    raw_data: Dict,
    user_id: str
) -> Dict:
    """
    规范化订阅字段（带缓存）
    
    Args:
        raw_data: 原始字段 dict，必须包含：
            - buyer_name
            - seller_name
            - plan_name
            - currency
            - amount
        user_id: 用户 ID
        
    Returns:
        规范化后的字段 dict（可能与原始相同）
    """
    logger.info(f"Normalizing subscription for user: {user_id}")
    
    # 1. 生成 normalized_key
    normalized_key = generate_normalized_key(
        raw_data.get('buyer_name', ''),
        raw_data.get('seller_name', ''),
        raw_data.get('plan_name', ''),
        raw_data.get('currency', 'USD'),
        raw_data.get('amount', 0)
    )
    
    logger.info(f"Generated normalized_key: {normalized_key}")
    
    # 2. 尝试从缓存读取
    cache_key = f"canonical:{user_id}:{normalized_key}"
    
    try:
        cached = await redis_client.get(cache_key)
        if cached:
            logger.info(f"Cache hit for {normalized_key}")
            return json.loads(cached)
    except Exception as e:
        logger.warning(f"Cache read failed: {e}")
    
    # 3. 精确匹配
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(CanonicalEntities).where(
                CanonicalEntities.user_id == user_id,
                CanonicalEntities.normalized_key == normalized_key,
                CanonicalEntities.is_active == True
            )
        )
        canonical = result.scalar_one_or_none()
        
        if canonical:
            logger.info(f"Exact match found (canonical_id={canonical.id})")
            
            # 更新统计（异步，不阻塞）
            await session.execute(
                update(CanonicalEntities)
                .where(CanonicalEntities.id == canonical.id)
                .values(
                    match_count=CanonicalEntities.match_count + 1,
                    last_matched_at=datetime.utcnow()
                )
            )
            await session.commit()
            
            # 缓存结果
            cache_data = {
                'match_type': 'exact',
                'canonical_id': canonical.id,
                **raw_data
            }
            try:
                await redis_client.setex(
                    cache_key,
                    CACHE_TTL,
                    json.dumps(cache_data, default=str)
                )
            except Exception as e:
                logger.warning(f"Cache write failed: {e}")
            
            return {**raw_data, 'canonical_id': canonical.id}
        
    # 4. 模糊匹配
    logger.info("Attempting fuzzy match...")
    
    async with AsyncSessionLocal() as session:
        fuzzy_result = await session.execute(
            select(
                CanonicalEntities,
                func.similarity(CanonicalEntities.normalized_key, normalized_key).label('score')
            )
            .where(
                CanonicalEntities.user_id == user_id,
                CanonicalEntities.is_active == True,
                func.similarity(CanonicalEntities.normalized_key, normalized_key) > FUZZY_THRESHOLD
            )
            .order_by(
                func.similarity(CanonicalEntities.normalized_key, normalized_key).desc(),
                CanonicalEntities.match_count.desc()
            )
            .limit(1)
        )
        
        fuzzy_match = fuzzy_result.first()
    
    if fuzzy_match:
        similar, score = fuzzy_match[0], fuzzy_match[1]
        logger.info(f"Fuzzy match found (canonical_id={similar.id}, score={score:.3f})")
        
        # 更新统计
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(CanonicalEntities)
                .where(CanonicalEntities.id == similar.id)
                .values(
                    match_count=CanonicalEntities.match_count + 1,
                    last_matched_at=datetime.utcnow()
                )
            )
            await session.commit()
        
        # 返回规范字段
        normalized_data = {
            'buyer_name': decrypt_value(similar.canonical_buyer_name),
            'seller_name': decrypt_value(similar.canonical_seller_name),
            'plan_name': decrypt_value(similar.canonical_plan_name),
            'currency': similar.canonical_currency,
            'amount': float(similar.canonical_amount),
            'canonical_id': similar.id
        }
        
        # 缓存结果
        cache_data = {
            'match_type': 'fuzzy',
            'score': float(score),
            **normalized_data
        }
        try:
            await redis_client.setex(
                cache_key,
                CACHE_TTL,
                json.dumps(cache_data, default=str)
            )
        except Exception as e:
            logger.warning(f"Cache write failed: {e}")
        
        return normalized_data
    
    # 5. 无匹配 - 创建新规则
    logger.info("No match found, creating new canonical entity")
    
    async with AsyncSessionLocal() as session:
        new_canonical_result = await session.execute(
            insert(CanonicalEntities).values(
                user_id=user_id,
                canonical_buyer_name=encrypt_value(raw_data.get('buyer_name', '')),
                canonical_seller_name=encrypt_value(raw_data.get('seller_name', '')),
                canonical_plan_name=encrypt_value(raw_data.get('plan_name', '')),
                canonical_currency=raw_data.get('currency', 'USD'),
                canonical_amount=Decimal(str(raw_data.get('amount', 0))),
                normalized_key=normalized_key,
                match_count=1,
                last_matched_at=datetime.utcnow()
            ).returning(CanonicalEntities.id)
        )
        await session.commit()
    
        new_canonical_id = new_canonical_result.scalar_one()
        logger.info(f"Created new canonical entity (id={new_canonical_id})")
    
    # 缓存结果
    cache_data = {
        'match_type': 'exact',
        'canonical_id': new_canonical_id,
        **raw_data
    }
    try:
        await redis_client.setex(
            cache_key,
            CACHE_TTL,
            json.dumps(cache_data, default=str)
        )
    except Exception as e:
        logger.warning(f"Cache write failed: {e}")
    
    return {**raw_data, 'canonical_id': new_canonical_id}


async def invalidate_canonical_cache(user_id: str, normalized_key: str = None):
    """
    使缓存失效（用户修改 canonical 后调用）
    
    Args:
        user_id: 用户 ID
        normalized_key: 可选，指定键；为空则清除该用户所有缓存
    """
    try:
        if normalized_key:
            cache_key = f"canonical:{user_id}:{normalized_key}"
            await redis_client.delete(cache_key)
            logger.info(f"Invalidated cache: {cache_key}")
        else:
            # 清除该用户所有缓存
            pattern = f"canonical:{user_id}:*"
            cursor = 0
            while True:
                cursor, keys = await redis_client.scan(cursor, match=pattern, count=100)
                if keys:
                    await redis_client.delete(*keys)
                if cursor == 0:
                    break
            logger.info(f"Invalidated all canonical caches for user: {user_id}")
    except Exception as e:
        logger.warning(f"Cache invalidation failed: {e}")