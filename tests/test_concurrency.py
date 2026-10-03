"""一个页面长时间暂停时，其他分支继续推进；活跃数不超过 12；不提前完成。"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from helpers import ProviderHarness, static_limited_depths

from sentinel_crawl import crawl

MAX_DEPTH = 3
BRANCHES = 15


def build_graph() -> dict[str, list[str]]:
    graph: dict[str, list[str]] = {"seed": ["pause"] + [f"b{i}" for i in range(BRANCHES)]}
    graph["pause"] = []
    for i in range(BRANCHES):
        children = [f"b{i}_c{j}" for j in range(3)]
        graph[f"b{i}"] = children
        for j, child in enumerate(children):
            graph[child] = [f"b{i}_c{j}_l{k}" for k in range(2)]
    return graph


async def test_paused_page_does_not_block_others():
    graph = build_graph()
    total_pages = 1 + 1 + BRANCHES + BRANCHES * 3 + BRANCHES * 3 * 2
    harness = ProviderHarness(graph, batch_size=4)
    pause_gate = asyncio.Event()
    harness.set_gate("pause", pause_gate)

    crawl_task = asyncio.create_task(
        crawl("seed", harness.factory(), max_depth=MAX_DEPTH)
    )

    # 等待：除暂停页外所有页面都已启动且交付结束（只剩暂停页占 1 个活跃名额）
    while not (len(harness.starts) == total_pages and harness.active == 1):
        await asyncio.sleep(0)
        assert harness.active <= 12

    # 其他分支在暂停解除前已推进到底
    assert "b14_c2_l1" in harness.starts
    # 尚有页面未交付结束，任务不得报告完成
    assert not crawl_task.done()
    assert harness.max_active == 12  # 暂停期间其余 11 个名额被用满

    pause_gate.set()
    result = await crawl_task

    assert harness.max_active <= 12
    assert result.depths == static_limited_depths("seed", graph, MAX_DEPTH)
    assert all(count == 1 for count in harness.starts.values())
