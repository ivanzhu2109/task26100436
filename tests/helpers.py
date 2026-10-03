"""测试辅助：静态限深可达计算与可控页面提供器。"""

from __future__ import annotations

import asyncio
from collections import deque


def static_limited_depths(seed: str, graph: dict[str, list[str]], max_depth: int) -> dict[str, int]:
    """独立静态计算：从种子出发、深度不超过 max_depth 的最短路径深度。"""
    depths = {seed: 0}
    queue = deque([seed])
    while queue:
        page = queue.popleft()
        next_depth = depths[page] + 1
        if next_depth > max_depth:
            continue
        for link in graph.get(page, []):
            if link not in depths:
                depths[link] = next_depth
                queue.append(link)
    return depths


def shortest_paths_over(seed: str, nodes: set[str], graph: dict[str, list[str]]) -> dict[str, int]:
    """只经过 nodes 集合内页面的最短路径深度（不可达的节点不出现）。"""
    depths = {seed: 0}
    queue = deque([seed])
    while queue:
        page = queue.popleft()
        for link in graph.get(page, []):
            if link in nodes and link not in depths:
                depths[link] = depths[page] + 1
                queue.append(link)
    return depths


class ProviderHarness:
    """按图生成异步页面提供器，支持分批、暂停门控与启动/并发统计。"""

    def __init__(self, graph: dict[str, list[str]], batch_size: int = 7):
        self.graph = graph
        self.batch_size = batch_size
        self.starts: dict[str, int] = {}
        self.active = 0
        self.max_active = 0
        self.gates: dict[str, asyncio.Event] = {}
        self.deferred: dict[str, tuple[asyncio.Event, list[str]]] = {}

    def set_gate(self, url: str, gate: asyncio.Event) -> None:
        """令 url 的提供器在交付完链接后等待 gate 再结束。"""
        self.gates[url] = gate

    def defer_links(self, url: str, gate: asyncio.Event, links: list[str]) -> None:
        """令 url 的提供器先交付图内链接，再等待 gate 后补交 links。"""
        self.deferred[url] = (gate, links)

    def factory(self):
        async def provider(url: str):
            self.starts[url] = self.starts.get(url, 0) + 1
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            try:
                links = self.graph.get(url, [])
                for i in range(0, len(links), self.batch_size):
                    yield links[i : i + self.batch_size]
                    await asyncio.sleep(0)
                deferred = self.deferred.get(url)
                if deferred is not None:
                    gate, extra = deferred
                    await gate.wait()
                    for i in range(0, len(extra), self.batch_size):
                        yield extra[i : i + self.batch_size]
                        await asyncio.sleep(0)
                gate = self.gates.get(url)
                if gate is not None:
                    await gate.wait()
            finally:
                self.active -= 1

        return provider
