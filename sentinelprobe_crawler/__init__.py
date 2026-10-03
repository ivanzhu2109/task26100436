"""SentinelProbe 并发限深爬取库。"""
from .crawler import CrawlTask, MAX_CONCURRENCY, MAX_URLS
from .types import CrawlResult, PageProvider

__all__ = [
    "CrawlTask",
    "CrawlResult",
    "PageProvider",
    "MAX_URLS",
    "MAX_CONCURRENCY",
]
