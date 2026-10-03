# sentinel-crawl

SentinelProbe 扫描任务使用的并发限深爬取库。任务状态仅保存在内存中。

## 用法

```python
from sentinel_crawl import crawl

async def provider(url: str):
    """页面提供器：逐批交付链接的异步迭代器，批次之间可以暂停。"""
    yield ["https://target/a", "https://target/b"]
    yield ["https://target/c"]

result = await crawl(
    "https://target/",          # 种子 URL，深度为 0
    lambda url: provider(url),  # 页面提供器工厂，每个已接纳 URL 恰好调用一次
    max_depth=3,                # 深度上限，1 至 5
)
# result.depths: dict[str, int] —— 所有已接纳 URL 及其最小深度
```

## 参数

- `max_depth`：深度上限（1–5），沿一条链接深度加 1。
- `max_pages`：单次任务最多接纳的不同 URL 数，默认 10000（含种子，名额不回收）。
- `max_concurrency`：同时活跃的页面提供器上限，默认 12（从启动到交付结束计入）。

## 行为保证

- 每个已接纳 URL 恰好启动一次页面提供器；达到深度上限的页面也完整接收链接。
- 深度按只经过已接纳页面的最短链接路径计算；短路径晚到时修正该页面及其后继的
  深度，并接纳因此进入深度范围的新页面，且不因此重启任何提供器。
- 满额后不再接纳新 URL，但仍处理指向已接纳页面的链接并修正深度。
- 全部已接纳页面交付结束且所有深度修正处理完毕后，任务才结束。

## 测试

```bash
python3 -m pytest
```
