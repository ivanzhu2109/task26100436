"""短路径晚到时的深度修正：页面已结束、页面仍在交付两种情形。"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from helpers import static_limited_depths

from sentinel_crawl import crawl

MAX_DEPTH = 4

# seed -> a -> b -> c -> x（长路径，x 深度 4），x -> y（深度 5 超限，先被遗漏）
# seed -> x（短路径晚到，x 修正为 1，y 应被找回为 2）
GRAPH = {
    "seed": ["a", "x"],
    "a": ["b"],
    "b": ["c"],
    "c": ["x"],
    "x": ["y"],
    "y": [],
}


async def test_short_path_after_page_finished():
    """x 的提供器已交付结束，短路径才到达：仍须修正并找回 y。"""
    x_done = asyncio.Event()
    starts: dict[str, int] = {}

    async def provider(url: str):
        starts[url] = starts.get(url, 0) + 1
        if url == "seed":
            yield ["a"]  # 先走长路径
            await x_done.wait()  # 等 x 的提供器完全结束
            yield ["x"]  # 短路径晚到
        elif url == "x":
            yield ["y"]
            x_done.set()
        else:
            yield GRAPH[url]

    result = await crawl("seed", provider, max_depth=MAX_DEPTH)

    assert result.depths == static_limited_depths("seed", GRAPH, MAX_DEPTH)
    assert result.depths["x"] == 1
    assert result.depths["y"] == 2  # 先前因超限遗漏，修正后被找回
    assert starts["x"] == 1  # 不得为修正深度再次启动提供器
    assert starts["y"] == 1


async def test_short_path_while_page_still_delivering():
    """x 的提供器暂停在批次之间时短路径到达：修正立即生效，后续批次按新深度处理。"""
    x_started = asyncio.Event()
    x_resume = asyncio.Event()
    starts: dict[str, int] = {}

    async def provider(url: str):
        starts[url] = starts.get(url, 0) + 1
        if url == "seed":
            yield ["a"]
            await x_started.wait()  # 等 x 经长路径被接纳并启动
            yield ["x"]  # x 仍在交付途中，短路径到达
        elif url == "x":
            yield ["y", "z"]  # 交付一批后暂停
            x_started.set()
            await x_resume.wait()
            yield ["w"]  # 修正后的后续批次
        else:
            yield GRAPH.get(url, [])

    async def release_when_corrected():
        # 等 y 被找回（说明修正已发生）后放行 x 的后续批次
        while "y" not in shared_depths:
            await asyncio.sleep(0)
        x_resume.set()

    shared_depths: dict[str, int] = {}
    import sentinel_crawl.crawler as crawler_mod

    original_admit = crawler_mod._CrawlTask._admit

    def spy_admit(self, url, depth):
        original_admit(self, url, depth)
        shared_depths[url] = depth

    crawler_mod._CrawlTask._admit = spy_admit
    try:
        releaser = asyncio.create_task(release_when_corrected())
        result = await crawl("seed", provider, max_depth=MAX_DEPTH)
        await releaser
    finally:
        crawler_mod._CrawlTask._admit = original_admit

    graph = {**GRAPH, "x": ["y", "z", "w"]}
    assert result.depths == static_limited_depths("seed", graph, MAX_DEPTH)
    assert result.depths["x"] == 1
    assert result.depths["y"] == 2
    assert result.depths["z"] == 2
    assert result.depths["w"] == 2  # 暂停解除后的批次按修正深度处理
    assert starts["x"] == 1
