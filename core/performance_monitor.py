import time
import asyncio
import logging
from typing import Optional, Callable, Any
from functools import wraps
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class PerformanceMonitor:
    """性能监控工具"""
    
    def __init__(self):
        self.metrics = {}
    
    def record(self, metric_name: str, value: float, tags: dict = None):
        """
        记录性能指标
        
        Args:
            metric_name: 指标名称
            value: 指标值
            tags: 标签字典
        """
        if metric_name not in self.metrics:
            self.metrics[metric_name] = []
        
        self.metrics[metric_name].append({
            "value": value,
            "timestamp": time.time(),
            "tags": tags or {}
        })
    
    def get_stats(self, metric_name: str) -> dict:
        """
        获取指标统计信息
        
        Args:
            metric_name: 指标名称
            
        Returns:
            统计字典 {min, max, avg, count}
        """
        if metric_name not in self.metrics:
            return {}
        
        values = [m["value"] for m in self.metrics[metric_name]]
        
        return {
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "count": len(values)
        }
    
    def clear(self):
        """清空所有指标"""
        self.metrics.clear()


# 全局监控实例
monitor = PerformanceMonitor()


def timer(metric_name: str = None):
    """
    装饰器: 自动计时异步函数
    
    Args:
        metric_name: 指标名称 (默认使用函数名)
    
    使用示例:
    ```python
    @timer("ocr_processing")
    async def process_ocr(file_url: str):
        ...
    ```
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            name = metric_name or f"{func.__module__}.{func.__name__}"
            start = time.time()
            
            try:
                result = await func(*args, **kwargs)
                elapsed = time.time() - start
                monitor.record(name, elapsed, {"status": "success"})
                logger.info(f"⏱️ {name} completed in {elapsed:.2f}s")
                return result
                
            except Exception as e:
                elapsed = time.time() - start
                monitor.record(name, elapsed, {"status": "error"})
                logger.error(f"❌ {name} failed after {elapsed:.2f}s: {e}")
                raise
        
        return wrapper
    return decorator


@asynccontextmanager
async def measure_time(operation_name: str):
    """
    上下文管理器: 测量代码块执行时间
    
    使用示例:
    ```python
    async with measure_time("database_query"):
        result = await session.execute(query)
    ```
    """
    start = time.time()
    try:
        yield
    finally:
        elapsed = time.time() - start
        logger.info(f"⏱️ {operation_name} took {elapsed:.2f}s")
        monitor.record(operation_name, elapsed)


class RateLimiter:
    """异步速率限制器"""
    
    def __init__(self, max_calls: int, period: float):
        """
        初始化速率限制器
        
        Args:
            max_calls: 时间窗口内最大调用次数
            period: 时间窗口(秒)
        """
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = asyncio.Lock()
    
    async def acquire(self):
        """获取令牌 (阻塞直到可用)"""
        async with self.lock:
            now = time.time()
            
            # 清理过期调用
            self.calls = [c for c in self.calls if now - c < self.period]
            
            # 如果超过限制，等待
            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now - self.calls[0])
                if sleep_time > 0:
                    logger.debug(f"Rate limit reached, sleeping {sleep_time:.2f}s")
                    await asyncio.sleep(sleep_time)
                    return await self.acquire()
            
            self.calls.append(now)
    
    async def __aenter__(self):
        await self.acquire()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class ConcurrencyLimiter:
    """并发限制器 (比 Semaphore 更智能)"""
    
    def __init__(self, max_concurrent: int, name: str = "limiter"):
        """
        初始化并发限制器
        
        Args:
            max_concurrent: 最大并发数
            name: 限制器名称
        """
        self.max_concurrent = max_concurrent
        self.name = name
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.active_count = 0
        self.total_acquired = 0
        self.lock = asyncio.Lock()
    
    @asynccontextmanager
    async def acquire(self):
        """获取并发槽位"""
        async with self.semaphore:
            async with self.lock:
                self.active_count += 1
                self.total_acquired += 1
                current = self.active_count
            
            logger.debug(f"{self.name}: acquired ({current}/{self.max_concurrent} active)")
            
            try:
                yield
            finally:
                async with self.lock:
                    self.active_count -= 1
                    current = self.active_count
                
                logger.debug(f"{self.name}: released ({current}/{self.max_concurrent} active)")
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        return {
            "max_concurrent": self.max_concurrent,
            "active_count": self.active_count,
            "total_acquired": self.total_acquired
        }


class MemoryMonitor:
    """内存监控器"""
    
    @staticmethod
    def get_memory_usage() -> dict:
        """
        获取当前内存使用情况
        
        Returns:
            内存使用字典 (MB)
        """
        try:
            import psutil
            process = psutil.Process()
            mem_info = process.memory_info()
            
            return {
                "rss_mb": mem_info.rss / 1024 / 1024,  # 物理内存
                "vms_mb": mem_info.vms / 1024 / 1024,  # 虚拟内存
                "percent": process.memory_percent()     # 内存占用百分比
            }
        except ImportError:
            logger.warning("psutil not installed, memory monitoring unavailable")
            return {}
    
    @staticmethod
    def log_memory():
        """记录当前内存使用"""
        mem = MemoryMonitor.get_memory_usage()
        if mem:
            logger.info(f"💾 Memory: {mem['rss_mb']:.1f}MB (RSS), {mem['percent']:.1f}%")


# 全局限制器实例
ocr_limiter = ConcurrencyLimiter(5, "OCR")
upload_limiter = ConcurrencyLimiter(10, "Upload")
download_limiter = ConcurrencyLimiter(10, "Download")