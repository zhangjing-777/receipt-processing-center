import aiohttp
import logging
from core.config import settings


logger = logging.getLogger(__name__)


async def send_push(payload: dict):
    async with aiohttp.ClientSession() as session:
        async with session.post(settings.push_url, json=payload) as resp:
            res = await resp.json() 
            logger.info(f"Status:, {resp.status}")
            logger.info(f"Response:, {res}")
