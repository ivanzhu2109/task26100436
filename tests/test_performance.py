"""性能冒烟：约 1 万页面、20 万条链接交付量，10 秒内完成。"""

from __future__ import annotations

import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from helpers import ProviderHarness, static_limited_depths

from sentinel_crawl import crawl


async def test_full_scale_within_budget():
    rng = random.Random(7)
    graph: dict[str, list[str]] = {"seed": [f"d1_{i}" for i in range(100)]}
    for i in range(100):
        graph[f"d1_{i}"] = [f"d2_{i * 98 + j}" for j in range(98)]
    d2_nodes = [f"d2_{k}" for k in range(9800)]
    all_nodes = ["seed"] + [f"d1_{i}" for i in range(100)] + d2_nodes
    for node in d2_nodes:  # 每个叶子再交付 20 条指回已接纳页面的链接（环）
        graph[node] = rng.choices(all_nodes, k=20)
    # 9901 个页面，约 100 + 9800 + 196000 = 205900 条链接

    harness = ProviderHarness(graph, batch_size=50)
    started_at = time.monotonic()
    result = await crawl("seed", harness.factory(), max_depth=5)
    elapsed = time.monotonic() - started_at

    assert result.depths == static_limited_depths("seed", graph, 5)
    assert len(result.depths) == 9901
    assert harness.max_active <= 12
    assert elapsed < 10, f"任务耗时 {elapsed:.1f}s，超过 10s 预算"
