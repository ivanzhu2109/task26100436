"""同一图上改变批次交付顺序，结果必须等于独立静态计算的限深可达结果。"""

from __future__ import annotations

import random
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from helpers import ProviderHarness, static_limited_depths

from sentinel_crawl import crawl


def random_graph(rng: random.Random, n: int, extra_edges: int) -> dict[str, list[str]]:
    nodes = [f"u{i}" for i in range(n)]
    graph: dict[str, list[str]] = {node: [] for node in nodes}
    for i in range(1, n):  # 保证连通
        graph[rng.choice(nodes[:i])].append(nodes[i])
    for _ in range(extra_edges):  # 随机加边，允许环与回指种子
        graph[rng.choice(nodes)].append(rng.choice(nodes))
    return graph


def shuffled_graph(rng: random.Random, graph: dict[str, list[str]]) -> dict[str, list[str]]:
    return {node: rng.sample(links, len(links)) for node, links in graph.items()}


@pytest.mark.parametrize("max_depth", [1, 2, 3, 4, 5])
async def test_random_graph_matches_static(max_depth: int):
    rng = random.Random(42 + max_depth)
    graph = random_graph(rng, n=120, extra_edges=300)
    expected = static_limited_depths("u0", graph, max_depth)
    assert len(expected) < 10_000

    for trial in range(4):  # 同一图，多种批次/链接顺序
        ordered = shuffled_graph(random.Random(1000 + trial), graph)
        harness = ProviderHarness(ordered, batch_size=1 + trial)
        result = await crawl("u0", harness.factory(), max_depth=max_depth)
        assert result.depths == expected
        assert all(count == 1 for count in harness.starts.values())
        assert set(harness.starts) == set(expected)
