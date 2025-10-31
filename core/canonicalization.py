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
    规范化订阅字段（带缓存 + 模糊匹配 + UPSERT）
    
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
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    
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
    
    # 3. 模糊匹配：查找相似的 normalized_key
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
    
    # 4. 如果模糊匹配成功，使用已有的 normalized_key
    final_normalized_key = normalized_key
    use_existing_canonical = False
    
    if fuzzy_match:
        similar, score = fuzzy_match[0], fuzzy_match[1]
        logger.info(f"Fuzzy match found (canonical_id={similar.id}, score={score:.3f})")
        
        # 使用已存在的 normalized_key，确保插入到同一规范记录
        final_normalized_key = similar.normalized_key
        use_existing_canonical = True
        
        # 返回规范化字段（从已存在记录）
        normalized_data = {
            'buyer_name': decrypt_value(similar.canonical_buyer_name),
            'seller_name': decrypt_value(similar.canonical_seller_name),
            'plan_name': decrypt_value(similar.canonical_plan_name),
            'currency': similar.canonical_currency,
            'amount': float(similar.canonical_amount),
            'canonical_id': similar.id
        }
    else:
        logger.info("No fuzzy match found, will create/update with original key")
        # 使用原始数据
        normalized_data = raw_data.copy()
    
    # 5. UPSERT：插入或更新（原子操作，无竞态条件）
    async with AsyncSessionLocal() as session:
        try:
            stmt = pg_insert(CanonicalEntities).values(
                user_id=user_id,
                canonical_buyer_name=encrypt_value(normalized_data.get('buyer_name', '')),
                canonical_seller_name=encrypt_value(normalized_data.get('seller_name', '')),
                canonical_plan_name=encrypt_value(normalized_data.get('plan_name', '')),
                canonical_currency=normalized_data.get('currency', 'USD'),
                canonical_amount=Decimal(str(normalized_data.get('amount', 0))),
                normalized_key=final_normalized_key,
                match_count=1,
                last_matched_at=datetime.utcnow(),
                is_active=True,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            ).on_conflict_do_update(
                index_elements=['normalized_key'],  # 基于唯一约束
                set_={
                    'match_count': CanonicalEntities.match_count + 1,
                    'last_matched_at': datetime.utcnow(),
                    'updated_at': datetime.utcnow()
                }
            ).returning(CanonicalEntities.id)
            
            result = await session.execute(stmt)
            await session.commit()
            
            canonical_id = result.scalar_one()
            logger.info(f"UPSERT completed (canonical_id={canonical_id})")
            
        except Exception as e:
            await session.rollback()
            logger.exception(f"UPSERT failed: {e}")
            raise
    
    # 6. 准备返回数据
    normalized_data['canonical_id'] = canonical_id
    
    # 7. 缓存结果
    cache_data = {
        'match_type': 'fuzzy' if use_existing_canonical else 'exact',
        'canonical_id': canonical_id,
        **normalized_data
    }
    
    try:
        await redis_client.setex(
            cache_key,
            CACHE_TTL,
            json.dumps(cache_data, default=str)
        )
        logger.info(f"Cached result for {normalized_key}")
    except Exception as e:
        logger.warning(f"Cache write failed: {e}")
    
    return normalized_data


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