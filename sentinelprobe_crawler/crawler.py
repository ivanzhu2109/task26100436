"""并发限深爬取任务。

核心约束：
- 每个已接纳 URL 恰好启动一次页面提供器；
- 短路径晚到时修正最小深度并沿已发现链接传播，但不重启提供器；
- 名额（含等待中与处理中的页面）不回收，满额后只做深度修正；
- 全部提供器交付结束且深度修正传播完毕后任务才结束。
"""
from __future__ import annotations

import asyncio
from collections import deque
from typing import Dict, List

from .types import CrawlResult, PageProvider

MAX_URLS = 10000
MAX_CONCURRENCY = 12


class CrawlTask:
    """单次爬取任务：一个种子 URL，状态仅保存在内存中。"""

    def __init__(
        self,
        seed: str,
        provider: PageProvider,
        max_depth: int,
        max_urls: int = MAX_URLS,
        max_concurrency: int = MAX_CONCURRENCY,
    ) -> None:
        if not 1 <= max_depth <= 5:
            raise ValueError("max_depth 必须在 1 至 5 之间")
        if max_urls < 1:
            raise ValueError("max_urls 必须至少为 1（种子占一个名额）")
        if max_concurrency < 1:
            raise ValueError("max_concurrency 必须至少为 1")
        self.seed = seed
        self.provider = provider
        self.max_depth = max_depth
        self.max_urls = max_urls
        self.max_concurrency = max_concurrency

        # 任务状态（仅内存，随任务结束丢弃）
        self._depths: Dict[str, int] = {}  # 已接纳 URL -> 当前已知最小深度
        self._out_links: Dict[str, List[str]] = {}  # 已接纳 URL -> 已交付的全部链接
        self._started: Dict[str, int] = {}  # URL -> 提供器启动次数
        self._pending: deque[str] = deque()  # 已接纳、等待启动的 URL
        self._active = 0  # 正在运行的提供器数量
        self._changed = asyncio.Event()  # 有待启动页面或提供器结束时置位

    async def run(self) -> CrawlResult:
        """执行爬取，直到所有已接纳页面交付结束且深度修正传播完毕。"""
        self._accept(self.seed, 0)
        while True:
            while self._pending and self._active < self.max_concurrency:
                self._launch(self._pending.popleft())
            if not self._pending and self._active == 0:
                break
            self._changed.clear()
            # 启动新提供器后可能立刻又有名额，先再试一轮再等待。
            if self._pending and self._active < self.max_concurrency:
                continue
            await self._changed.wait()
        return CrawlResult(depths=dict(self._depths), started=dict(self._started))

    # ---- 内部状态更新（均在事件循环内同步执行，无需加锁） ----

    def _accept(self, url: str, depth: int) -> None:
        """接纳新 URL：登记深度、占用名额并排队等待启动。"""
        self._depths[url] = depth
        self._pending.append(url)
        self._changed.set()

    def _handle_link(self, source: str, target: str) -> None:
        """处理一条从已接纳页面交付出的链接。"""
        self._out_links.setdefault(source, []).append(target)
        new_depth = self._depths[source] + 1
        known = self._depths.get(target)
        if known is not None:
            if new_depth < known:
                self._depths[target] = new_depth
                self._propagate(target)
        elif new_depth <= self.max_depth and len(self._depths) < self.max_urls:
            self._accept(target, new_depth)

    def _propagate(self, url: str) -> None:
        """url 深度变小后，沿其已发现链接修正后继深度。

        后继可能已结束或仍在交付，均只更新内存中的深度，不重启提供器；
        因深度下降而进入范围的新 URL 在此被接纳。
        """
        stack = [url]
        while stack:
            current = stack.pop()
            next_depth = self._depths[current] + 1
            for link in self._out_links.get(current, ()):
                known = self._depths.get(link)
                if known is not None:
                    if next_depth < known:
                        self._depths[link] = next_depth
                        stack.append(link)
                elif next_depth <= self.max_depth and len(self._depths) < self.max_urls:
                    self._accept(link, next_depth)

    # ---- 提供器生命周期 ----

    def _launch(self, url: str) -> None:
        self._started[url] = self._started.get(url, 0) + 1
        self._active += 1
        asyncio.get_running_loop().create_task(self._run_provider(url))

    async def _run_provider(self, url: str) -> None:
        try:
            async for batch in self.provider(url):
                for link in batch:
                    self._handle_link(url, link)
        finally:
            self._active -= 1
            self._changed.set()
