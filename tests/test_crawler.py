"""CrawlTask 验收测试。"""
from __future__ import annotations

import asyncio
from collections import Counter, deque
from typing import AsyncIterator, Dict, List, Union

import pytest

from sentinelprobe_crawler import CrawlTask

pytestmark = pytest.mark.asyncio

Batch = Union[List[str], asyncio.Event]
Script = Dict[str, List[Batch]]


class Tracker:
    """记录提供器启动次数与并发峰值。"""

    def __init__(self) -> None:
        self.starts: Counter[str] = Counter()
        self.finished: set[str] = set()
        self.active = 0
        self.max_active = 0

    def provider(self, script: Script):
        async def run(url: str) -> AsyncIterator[List[str]]:
            self.starts[url] += 1
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            try:
                for batch in script.get(url, []):
                    if isinstance(batch, asyncio.Event):
                        await batch.wait()
                    else:
                        await asyncio.sleep(0)
                        yield batch
            finally:
                self.active -= 1
                self.finished.add(url)

        return run


async def wait_until(pred, timeout: float = 5.0) -> None:
    loops = int(timeout / 0.005)
    for _ in range(loops):
        if pred():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("wait_until 超时")


def static_limit(graph: Dict[str, List[str]], seed: str, max_depth: int) -> Dict[str, int]:
    """独立静态计算：限深 BFS 的可达集合与最小深度。"""
    depth = {seed: 0}
    queue = deque([seed])
    while queue:
        u = queue.popleft()
        if depth[u] == max_depth:
            continue
        for v in graph.get(u, []):
            if v not in depth:
                depth[v] = depth[u] + 1
                queue.append(v)
    return depth


def depths_over_accepted(
    graph: Dict[str, List[str]], seed: str, accepted: set[str], max_depth: int
) -> Dict[str, int]:
    """只经过已接纳页面的最短路径深度。"""
    depth = {seed: 0}
    queue = deque([seed])
    while queue:
        u = queue.popleft()
        if depth[u] == max_depth:
            continue
        for v in graph.get(u, []):
            if v in accepted and v not in depth:
                depth[v] = depth[u] + 1
                queue.append(v)
    return depth


# ---- 场景 1：同一张图改变批次交付顺序，结果恒等于静态限深可达 ----

GRAPH = {
    "S": ["a1", "a2", "a3"],
    "a1": ["b1", "b2"],
    "a2": ["b2", "b3"],
    "a3": ["b3"],
    "b1": ["c1"],
    "b2": ["c1", "c2"],
    "b3": ["c2", "S"],  # 指回种子，形成环
    "c1": ["c2", "a1"],  # 环
    "c2": [],
}


def batchings(links: List[str], mode: str) -> List[List[str]]:
    if mode == "single":
        return [links]
    if mode == "each":
        return [[x] for x in links]
    if mode == "reverse":
        return [[x] for x in reversed(links)]
    raise ValueError(mode)


@pytest.mark.parametrize("mode", ["single", "each", "reverse"])
async def test_batch_order_invariant(mode: str) -> None:
    tracker = Tracker()
    script = {url: batchings(links, mode) for url, links in GRAPH.items()}
    for max_depth in (1, 2, 3, 5):
        result = await CrawlTask("S", tracker.provider(script), max_depth).run()
        assert result.depths == static_limit(GRAPH, "S", max_depth)
        assert all(n == 1 for n in result.started.values())
        assert tracker.max_active <= 12


# ---- 场景 2：短路径晚到，页面已结束 ----


async def test_late_short_path_after_page_finished() -> None:
    tracker = Tracker()
    gate_s = asyncio.Event()
    script: Script = {
        "S": [["A"], gate_s, ["P"]],
        "A": [["B"]],
        "B": [["P"]],
        "P": [["Q"]],
    }
    task = asyncio.ensure_future(CrawlTask("S", tracker.provider(script), 3).run())
    # 长路径 S->A->B->P 走完，P 已结束；Q 因深度 4 超限被遗漏。
    await wait_until(lambda: "P" in tracker.finished)
    assert "Q" not in tracker.starts
    gate_s.set()  # 短路径 S->P 到达
    result = await task
    assert result.depths == {"S": 0, "A": 1, "B": 2, "P": 1, "Q": 2}
    assert result.started["P"] == 1  # 修正深度不重启提供器


# ---- 场景 3：短路径晚到，页面仍在交付 ----


async def test_late_short_path_while_page_delivering() -> None:
    tracker = Tracker()
    gate_s = asyncio.Event()
    gate_p = asyncio.Event()
    script: Script = {
        "S": [["A"], gate_s, ["P"]],
        "A": [["B"]],
        "B": [["P"]],
        "P": [["Q"], gate_p],  # P 交付 Q 后暂停，保持活跃
    }
    task = asyncio.ensure_future(CrawlTask("S", tracker.provider(script), 3).run())
    await wait_until(lambda: "P" in tracker.starts and "P" not in tracker.finished)
    assert "Q" not in tracker.starts  # Q 沿长路径深度 4，被遗漏
    gate_s.set()  # P 仍在交付时短路径到达
    await wait_until(lambda: "Q" in tracker.starts)  # Q 被找回，无需等 P 结束
    assert "P" not in tracker.finished
    gate_p.set()
    result = await task
    assert result.depths == {"S": 0, "A": 1, "B": 2, "P": 1, "Q": 2}
    assert result.started["P"] == 1


# ---- 场景 4：超过 10000 候选、满额后出现更短路径 ----


async def test_capacity_full_then_shorter_path() -> None:
    tracker = Tracker()
    gate_full = asyncio.Event()
    candidates = [f"c{i}" for i in range(12000)]
    script: Script = {
        "S": [["A"], gate_full, ["c0"]],
        "A": [candidates[i : i + 500] for i in range(0, len(candidates), 500)],
        "c0": [["d0"]],
    }
    graph = {"S": ["A", "c0"], "A": candidates, "c0": ["d0"]}
    task = asyncio.ensure_future(CrawlTask("S", tracker.provider(script), 3).run())
    await wait_until(lambda: len(tracker.starts) == 10000)
    assert "d0" in tracker.starts  # c0 早启动，d0 在满额前已接纳（深度 3）
    gate_full.set()  # 满额后 S->c0 提供更短路径
    result = await asyncio.wait_for(task, timeout=10)

    assert len(result.depths) == 10000
    assert len(result.started) == 10000
    assert all(n == 1 for n in result.started.values())
    assert result.depths["c0"] == 1  # 满额后深度被修正
    assert result.depths["d0"] == 2  # 修正沿已发现链接传播
    accepted = set(result.depths)
    assert result.depths == depths_over_accepted(graph, "S", accepted, 3)
    assert tracker.max_active <= 12


# ---- 场景 5：一个页面长时间暂停，其他分支继续推进 ----


async def test_paused_page_does_not_block_others() -> None:
    tracker = Tracker()
    gate_p = asyncio.Event()
    script: Script = {"S": [["p", "b1"]], "p": [gate_p]}
    for i in range(1, 5):
        script[f"b{i}"] = [[f"b{i + 1}", f"leaf{i}"]]
    script["b5"] = [[]]
    task = asyncio.ensure_future(CrawlTask("S", tracker.provider(script), 5).run())

    # p 暂停期间，b 分支持续推进直到全部结束。
    await wait_until(lambda: {"b5", "leaf4"} <= tracker.finished)
    assert tracker.max_active <= 12
    await asyncio.sleep(0.05)
    assert not task.done()  # p 未结束，任务不得报告完成

    gate_p.set()
    result = await task
    expected = {"S": 0, "p": 1}
    for i in range(1, 6):
        expected[f"b{i}"] = i
    for i in range(1, 5):
        expected[f"leaf{i}"] = i + 1
    assert result.depths == expected
    assert tracker.max_active <= 12


# ---- 场景 6：规模与性能（10000 页面、约 20 万条链接）----


async def test_scale_and_performance() -> None:
    tracker = Tracker()
    pages = [f"u{i}" for i in range(9999)]
    script: Script = {"S": [pages]}
    for i, url in enumerate(pages):
        # 每个页面交付 20 条链接，指向其他候选页（环与重复均存在）。
        script[url] = [[pages[(i * 7 + k) % len(pages)] for k in range(20)]]
    start = asyncio.get_running_loop().time()
    result = await CrawlTask("S", tracker.provider(script), 2).run()
    elapsed = asyncio.get_running_loop().time() - start
    assert len(result.depths) == 10000
    assert result.depths["S"] == 0
    assert all(d == 1 for u, d in result.depths.items() if u != "S")
    assert all(n == 1 for n in result.started.values())
    assert tracker.max_active <= 12
    assert elapsed < 10, f"耗时 {elapsed:.2f}s 超过 10s 预算"
