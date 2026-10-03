"""公共类型定义：页面提供器协议与爬取结果。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import AsyncIterator, Callable, Dict, List

# 页面提供器：给定 URL，返回逐批交付链接的异步迭代器。
PageProvider = Callable[[str], AsyncIterator[List[str]]]


@dataclass
class CrawlResult:
    """一次爬取任务的结果。"""

    depths: Dict[str, int] = field(default_factory=dict)
    """所有已接纳 URL 及其最小深度。"""

    started: Dict[str, int] = field(default_factory=dict)
    """每个已接纳 URL 启动提供器的次数（恒为 1，供调用方/测试核对）。"""
