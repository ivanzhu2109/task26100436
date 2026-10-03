# task26100436

SentinelProbe 并发限深爬取库（纯内存、asyncio 实现）。

## 用法

```python
from sentinelprobe_crawler import CrawlTask

async def my_provider(url):
    """页面提供器：逐批交付链接（list[str]），批次之间可暂停。"""
    yield ["https://a/", "https://b/"]
    yield ["https://c/"]

result = await CrawlTask(
    seed="https://seed/",
    provider=my_provider,
    max_depth=3,        # 1..5
    max_urls=10000,     # 含种子，名额不回收
    max_concurrency=12, # 同时活跃的提供器上限
).run()

result.depths  # {url: 最小深度}，含所有已接纳 URL
result.started # {url: 提供器启动次数}，恒为 1
```

## 语义

- 每个已接纳 URL 恰好启动一次提供器；达到深度上限的页面仍完整接收链接。
- 短路径晚到时，修正该页面及后继的最小深度（沿已发现链接传播），并接纳
  因此进入深度范围的新页面；已结束或仍在交付的页面都适用，且不重启提供器。
- 满 10000 个名额后不再接纳新 URL，但仍处理指向已接纳页面的链接并修正深度。
- 仅当全部已接纳页面交付结束、所有深度修正传播完毕，任务才结束。

## 测试

```bash
python3 -m pytest -q
```
