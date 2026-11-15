"""同步管理器关键行为：缺号触发重同步、重复恰好一次、迟到快照不覆盖新盘口。"""
import asyncio

import pytest

from app.config import Settings
from app.orderbook import GapError, StaleSnapshotError
from app.sync_manager import SyncManager

SYMBOL = "CU2601"


def make_manager() -> SyncManager:
    return SyncManager(Settings(symbol=SYMBOL, source_http="http://x", source_ws="ws://x"))


def install(mgr: SyncManager, gen: int = 1):
    mgr.generation = gen
    snapshot = {"type": "snapshot", "symbol": SYMBOL, "seq": 100,
                "bids": [[69990, 5]], "asks": [[70010, 7]]}
    asyncio.run(mgr._install_snapshot(snapshot, [], gen))


def test_apply_increment_continuous_and_duplicate():
    mgr = make_manager()
    install(mgr)
    assert mgr.synced and mgr.book.seq == 100

    asyncio.run(mgr._apply_increment(
        {"type": "increment", "symbol": SYMBOL, "seq": 101,
         "changes": [{"side": "bid", "price": 69980, "qty": 3}]}))
    assert mgr.book.seq == 101

    # 重复投递：跳过，不重复应用
    asyncio.run(mgr._apply_increment(
        {"type": "increment", "symbol": SYMBOL, "seq": 101,
         "changes": [{"side": "bid", "price": 69980, "qty": 3}]}))
    assert mgr.book.seq == 101 and mgr.stats["duplicates_skipped"] == 1


def test_gap_raises_and_counts():
    mgr = make_manager()
    install(mgr)
    with pytest.raises(GapError):
        asyncio.run(mgr._apply_increment(
            {"type": "increment", "symbol": SYMBOL, "seq": 105,
             "changes": [{"side": "bid", "price": 1, "qty": 1}]}))
    assert mgr.stats["gaps"] == 1


def test_stale_snapshot_rejected_at_install():
    """重连前发起的快照（旧世代）不得恢复可用状态或覆盖新盘口。"""
    mgr = make_manager()
    install(mgr, gen=2)  # 当前世代 2，盘口已同步

    stale = {"type": "snapshot", "symbol": SYMBOL, "seq": 50,
             "bids": [[1, 1]], "asks": []}
    with pytest.raises(StaleSnapshotError):
        asyncio.run(mgr._install_snapshot(stale, [], gen=1))  # 旧世代的迟到快照

    assert mgr.synced is True                 # 可用状态未被改变
    assert mgr.book.seq == 100                # 新盘口未被覆盖
    assert mgr.book.bids == {69990: 5}
    assert mgr.stats["stale_snapshots"] == 1


def test_stale_snapshot_discarded_at_fetch():
    """HTTP 响应返回时世代已更替 -> 返回 None，不落簿。"""
    mgr = make_manager()

    class FakeResp:
        def raise_for_status(self): pass
        def json(self):
            return {"type": "snapshot", "symbol": SYMBOL, "seq": 1, "bids": [], "asks": []}

    class FakeHttp:
        async def get(self, *a, **k):
            mgr.generation = 9  # 模拟请求在途期间发生了重连
            return FakeResp()

    mgr._http = FakeHttp()
    mgr.generation = 1
    result = asyncio.run(mgr._fetch_snapshot(gen=1))
    assert result is None
    assert mgr.stats["stale_snapshots"] == 1
    assert mgr.synced is False
