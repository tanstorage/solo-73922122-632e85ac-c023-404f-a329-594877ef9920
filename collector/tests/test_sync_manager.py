"""同步管理器关键行为：缺号触发重同步、重复恰好一次、迟到快照不覆盖新盘口、
摘要核验失败定位首个异常序号、旧连接迟到帧/迟到核验不影响新连接。"""
import asyncio
import copy

import pytest

from app.config import Settings
from app.orderbook import DigestMismatchError, GapError, StaleSnapshotError
from app.sync_manager import SyncManager
from helpers import SYMBOL, Chain, chg


def make_manager() -> SyncManager:
    return SyncManager(Settings(symbol=SYMBOL, source_http="http://x", source_ws="ws://x"))


def install(mgr: SyncManager, gen: int = 1) -> Chain:
    """安装一份合法快照并返回对应该链的帧构造器（链头 = 快照 digest）。"""
    mgr.generation = gen
    chain = Chain()
    asyncio.run(mgr._install_snapshot(chain.snap(100, [[69990, 5]], [[70010, 7]]), [], gen))
    return chain


def test_apply_increment_continuous_and_duplicate():
    mgr = make_manager()
    chain = install(mgr)
    assert mgr.synced and mgr.book.seq == 100

    f101 = chain.inc(101, [chg("bid", 69980, 3)])
    asyncio.run(mgr._apply_increment(f101, gen=1))
    assert mgr.book.seq == 101
    assert mgr.stats["increments_verified"] == 1

    # 重复投递：跳过，不重复应用、不重复核验
    asyncio.run(mgr._apply_increment(copy.deepcopy(f101), gen=1))
    assert mgr.book.seq == 101 and mgr.stats["duplicates_skipped"] == 1
    assert mgr.stats["increments_verified"] == 1


def test_gap_raises_and_counts():
    mgr = make_manager()
    chain = install(mgr)
    with pytest.raises(GapError):
        asyncio.run(mgr._apply_increment(chain.inc(105, [chg("bid", 1, 1)]), gen=1))
    assert mgr.stats["gaps"] == 1


def test_digest_mismatch_marks_first_anomaly():
    """帧内容被改写 -> 核验失败，记录首个异常序号；重建后异常清空、留痕保留。"""
    mgr = make_manager()
    chain = install(mgr)
    bad = chain.inc(101, [chg("bid", 69980, 3)])
    bad["changes"][0]["qty"] = 999  # 传输途中被改写，digest 仍是原值

    with pytest.raises(DigestMismatchError) as ei:
        asyncio.run(mgr._apply_increment(bad, gen=1))
    assert ei.value.seq == 101
    assert mgr.anomaly["seq"] == 101            # /status 标明首个异常序号
    assert mgr.last_anomaly["seq"] == 101
    assert mgr.stats["verification_failures"] == 1

    install(mgr, gen=2)                          # 按原流程重建
    assert mgr.anomaly is None                   # 当前 episode 结束
    assert mgr.last_anomaly["seq"] == 101        # 留痕仍在
    assert mgr.status_view()["verification"]["chain_ok"] is True


def test_duplicate_frame_not_reverified():
    """重复帧不核验：即使重放帧摘要被弄脏，也按重复投递正常跳过。"""
    mgr = make_manager()
    chain = install(mgr)
    f101 = chain.inc(101, [chg("bid", 69980, 3)])
    asyncio.run(mgr._apply_increment(f101, gen=1))

    dirty = copy.deepcopy(f101)
    dirty["digest"] = "0" * 64
    asyncio.run(mgr._apply_increment(dirty, gen=1))  # seq <= last -> 直接跳过
    assert mgr.stats["duplicates_skipped"] == 1
    assert mgr.anomaly is None and mgr.stats["verification_failures"] == 0


def test_stale_generation_frame_ignored():
    """旧连接迟到的帧（含迟到核验）不得使新连接失步或误报异常。"""
    mgr = make_manager()
    chain = install(mgr, gen=2)  # 新连接世代 2 已同步

    valid101 = chain.inc(101, [chg("bid", 69980, 3)])
    asyncio.run(mgr._apply_increment(valid101, gen=1))  # 旧世代的迟到帧
    assert mgr.synced is True and mgr.book.seq == 100   # 未应用、未失步
    assert mgr.stats["stale_frames"] == 1

    bad101 = copy.deepcopy(valid101)
    bad101["changes"][0]["qty"] = 1  # 旧连接上的迟到核验（内容被改写）
    asyncio.run(mgr._apply_increment(bad101, gen=1))
    assert mgr.synced is True and mgr.book.seq == 100
    assert mgr.anomaly is None                          # 不得误报异常
    assert mgr.stats["verification_failures"] == 0
    assert mgr.stats["stale_frames"] == 2


def test_stale_snapshot_rejected_at_install():
    """重连前发起的快照（旧世代）不得恢复可用状态或覆盖新盘口。"""
    mgr = make_manager()
    install(mgr, gen=2)  # 当前世代 2，盘口已同步

    stale = Chain().snap(50, [[1, 1]], [])  # 旧世代的迟到快照（格式合法）
    with pytest.raises(StaleSnapshotError):
        asyncio.run(mgr._install_snapshot(stale, [], gen=1))

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
            return Chain().snap(1, [], [])

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
