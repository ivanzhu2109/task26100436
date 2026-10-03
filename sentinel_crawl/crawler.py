"""并发限深爬取核心实现。

任务状态全部保存在内存中：
- ``depths``: 已接纳 URL -> 当前已知最小深度（只经过已接纳页面的最短路径）。
- ``edges``: 已接纳页面 -> 已交付的全部链接（用于深度修正时重放）。
- 每个已接纳 URL 在接纳时立即创建一个 provider 任务（恰好一次），
  任务在信号量上排队等待启动；持有信号量期间计入活跃并发数。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import AsyncIterable, Callable, Iterable, TypeAlias

#: 页面提供器工厂：给定 URL，返回逐批交付链接的异步迭代器。
PageProviderFactory: TypeAlias = Callable[[str], AsyncIterable[Iterable[str]]]

DEFAULT_MAX_PAGES = 10_000
DEFAULT_MAX_CONCURRENCY = 12
MIN_DEPTH_LIMIT = 1
MAX_DEPTH_LIMIT = 5


@dataclass(frozen=True)
class CrawlResult:
    """爬取结果：所有已接纳 URL 及其最小深度。"""

    depths: dict[str, int]

    @property
    def urls(self) -> set[str]:
        return set(self.depths)


class _CrawlTask:
    def __init__(
        self,
        seed: str,
        provider_factory: PageProviderFactory,
        max_depth: int,
        max_pages: int,
        max_concurrency: int,
    ) -> None:
        self._provider_factory = provider_factory
        self._max_depth = max_depth
        self._max_pages = max_pages
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._seed = seed
        self.depths: dict[str, int] = {}
        self.edges: dict[str, list[str]] = {}
        self._inflight = 0
        self._idle = asyncio.Event()

    async def run(self) -> CrawlResult:
        self._admit(self._seed, 0)
        while self._inflight > 0:
            self._idle.clear()
            await self._idle.wait()
        return CrawlResult(depths=dict(self.depths))

    def _admit(self, url: str, depth: int) -> None:
        """接纳新 URL 并为其创建 provider 任务（恰好一次）。"""
        self.depths[url] = depth
        self._inflight += 1
        task = asyncio.create_task(self._run_provider(url))
        task.add_done_callback(self._provider_done)

    def _provider_done(self, task: asyncio.Task[None]) -> None:
        self._inflight -= 1
        task.result()  # 让异常立即暴露
        if self._inflight == 0:
            self._idle.set()

    async def _run_provider(self, url: str) -> None:
        async with self._semaphore:
            async for batch in self._provider_factory(url):
                links = list(batch)
                if links:
                    self._handle_links(url, links)

    def _handle_links(self, page: str, links: list[str]) -> None:
        """登记 page 新交付的链接，并按当前深度传播接纳/修正。"""
        self.edges.setdefault(page, []).extend(links)
        replay: list[str] = []
        self._visit_batch(page, links, replay)
        while replay:
            reprocessed = replay.pop()
            self._visit_batch(reprocessed, self.edges[reprocessed], replay)

    def _visit_batch(self, page: str, links: list[str], replay: list[str]) -> None:
        next_depth = self.depths[page] + 1
        for link in links:
            known = self.depths.get(link)
            if known is not None:
                if next_depth < known:
                    self.depths[link] = next_depth
                    if link in self.edges:
                        replay.append(link)
            elif next_depth <= self._max_depth and len(self.depths) < self._max_pages:
                self._admit(link, next_depth)


async def crawl(
    seed: str,
    provider_factory: PageProviderFactory,
    *,
    max_depth: int,
    max_pages: int = DEFAULT_MAX_PAGES,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
) -> CrawlResult:
    """从种子 URL 启动一次限深爬取任务，返回最终结果。

    - ``max_depth``: 深度上限，1 至 5；种子深度为 0。
    - ``max_pages``: 单次任务最多接纳的不同 URL 数（含种子），名额不回收。
    - ``max_concurrency``: 同时活跃的页面提供器上限（从启动到交付结束计入）。
    """
    if not MIN_DEPTH_LIMIT <= max_depth <= MAX_DEPTH_LIMIT:
        raise ValueError(f"max_depth 必须在 {MIN_DEPTH_LIMIT}..{MAX_DEPTH_LIMIT} 之间")
    if max_pages < 1:
        raise ValueError("max_pages 必须 >= 1")
    if max_concurrency < 1:
        raise ValueError("max_concurrency 必须 >= 1")
    task = _CrawlTask(seed, provider_factory, max_depth, max_pages, max_concurrency)
    return await task.run()
