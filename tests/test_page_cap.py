"""名额上限：超过 10000 个候选并发出现、满额后又出现更短路径。"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from helpers import ProviderHarness, shortest_paths_over

from sentinel_crawl import crawl

MAX_DEPTH = 5
MAX_PAGES = 10_000


def build_graph() -> dict[str, list[str]]:
    graph: dict[str, list[str]] = {"seed": [f"d1_{i}" for i in range(50)]}
    for i in range(50):
        graph[f"d1_{i}"] = [f"d2_{i * 50 + j}" for j in range(50)]
    for j in range(2500):
        graph[f"d2_{j}"] = [f"d3_{j * 5 + k}" for k in range(5)]
    # 候选总数 1 + 50 + 2500 + 12500 = 15051，超过 10000 名额
    # d3_0 经长链被接纳于深度 3，并引出 mid(4) -> leaf(5)
    graph["d3_0"] = ["mid"]
    graph["mid"] = ["leaf"]
    graph["leaf"] = []
    return graph


async def test_cap_and_late_short_path():
    graph = build_graph()
    harness = ProviderHarness(graph, batch_size=3)
    cap_full = asyncio.Event()
    # d1_0 暂缓补交：满额后才给出指向 d3_0 的更短路径，以及一个全新 URL
    harness.defer_links("d1_0", cap_full, ["d3_0", "brand_new"])

    async def watcher():
        while len(harness.starts) < MAX_PAGES:
            await asyncio.sleep(0)
        cap_full.set()

    watcher_task = asyncio.create_task(watcher())
    started_at = time.monotonic()
    result = await crawl("seed", harness.factory(), max_depth=MAX_DEPTH)
    elapsed = time.monotonic() - started_at
    await watcher_task

    # 总启动数恰为 10000，每个 URL 只启动一次
    assert len(harness.starts) == MAX_PAGES
    assert all(count == 1 for count in harness.starts.values())
    assert len(result.depths) == MAX_PAGES

    # 满额后不再接纳新 URL，但仍修正已接纳页面的深度并级联传播
    assert "brand_new" not in result.depths
    assert result.depths["d3_0"] == 2  # 3 -> 2
    assert result.depths["mid"] == 3  # 4 -> 3
    assert result.depths["leaf"] == 4  # 5 -> 4

    # 最终深度与只经过已接纳页面的最短路径一致
    full_graph = dict(graph)
    full_graph["d1_0"] = graph["d1_0"] + ["d3_0", "brand_new"]
    accepted = set(result.depths)
    assert result.depths == shortest_paths_over("seed", accepted, full_graph)

    assert elapsed < 10, f"任务耗时 {elapsed:.1f}s，超过 10s 预算"
