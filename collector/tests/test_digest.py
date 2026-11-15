"""摘要链：算法已知答案、篡改定位首个异常序号、补偿性篡改无法掩盖。"""
import copy

import pytest

from app.digest import (
    ALGO,
    canonical_increment,
    chain_digest,
    genesis_digest,
    is_digest,
)
from app.orderbook import DigestMismatchError, build_synced_book
from helpers import SYMBOL, Chain, chg

# 算法已知答案（sha256-chain-v1）：算法变动必须同步更新协议版本，禁止静默漂移
GENESIS_CU2601 = "4ce7197d293d472edf1bb83894dd6d461056dea8f246492102e9e0f0e29453e7"
D1_CU2601 = "84c790b550c06877491bbd40b4d38e80c4ffee314e4d7cd659cc0f8e3f2a3594"
D2_CU2601 = "959e71e69bee230cff9e9e2435596d410e5b82dbd67c87681498132b26b5117f"


def test_known_answer_vectors():
    assert ALGO == "sha256-chain-v1"
    g = genesis_digest(SYMBOL)
    assert g == GENESIS_CU2601
    d1 = chain_digest(g, SYMBOL, 1, [chg("bid", 69990, 42)])
    assert d1 == D1_CU2601
    d2 = chain_digest(d1, SYMBOL, 2, [chg("ask", 70010, 0), chg("bid", 69980, 7)])
    assert d2 == D2_CU2601


def test_canonical_ignores_dict_key_order_and_extra_fields():
    a = canonical_increment(SYMBOL, 5, [{"side": "bid", "price": 100, "qty": 3}])
    b = canonical_increment(SYMBOL, 5, [{"qty": 3, "price": 100, "side": "bid"}])
    assert a == b
    assert is_digest(chain_digest(genesis_digest(SYMBOL), SYMBOL, 5,
                                  [{"side": "bid", "price": 100, "qty": 3}]))


def test_valid_chain_passes_and_head_matches():
    c = Chain()
    snapshot = c.snap(10, [[70000, 5]], [])
    frames = [c.inc(11, [chg("bid", 69990, 3)]),
              c.inc(12, [chg("ask", 70010, 8)]),
              c.inc(13, [chg("bid", 70000, 0)])]
    book, applied, dropped = build_synced_book(snapshot, frames, SYMBOL)
    assert (applied, dropped) == (3, 0)
    assert book.seq == 13 and book.digest == c.digest


def test_tampered_qty_locates_first_anomaly_seq():
    """第 12 号帧量被改 -> 首个异常序号为 12（其后的合法帧不再有机会）。"""
    c = Chain()
    snapshot = c.snap(10, [[70000, 5]], [])
    f11 = c.inc(11, [chg("bid", 69990, 3)])
    f12 = c.inc(12, [chg("bid", 69980, 7)])
    f13 = c.inc(13, [chg("ask", 70010, 8)])
    bad12 = copy.deepcopy(f12)
    bad12["changes"][0]["qty"] = 70  # 传输途中改量，digest 仍为原值
    with pytest.raises(DigestMismatchError) as ei:
        build_synced_book(snapshot, [f11, bad12, f13], SYMBOL)
    assert ei.value.seq == 12


def test_compensating_tamper_cannot_hide_first():
    """后续改量即使抵消盘口差异，首个异常序号仍被定位。

    12 号帧量 +10、13 号帧量 -10：最终盘口与未篡改一致，但链核验在 12 号即失败。
    """
    c = Chain()
    snapshot = c.snap(10, [[69990, 50]], [])
    f11 = c.inc(11, [chg("bid", 69980, 7)])
    f12 = c.inc(12, [chg("bid", 69990, 60)])   # 原始：50 -> 60
    f13 = c.inc(13, [chg("bid", 69990, 50)])   # 原始：60 -> 50
    bad12 = copy.deepcopy(f12)
    bad12["changes"][0]["qty"] = 70            # 篡改：50 -> 70
    bad13 = copy.deepcopy(f13)
    bad13["changes"][0]["qty"] = 50            # 篡改：70 -> 50（恰好抵消盘口差异）
    # 未核验时盘口将回到 {69990: 50}，与未篡改一致 —— 但链不会说谎
    with pytest.raises(DigestMismatchError) as ei:
        build_synced_book(snapshot, [f11, bad12, bad13], SYMBOL)
    assert ei.value.seq == 12                  # 首个异常序号 = 12


def test_tampered_price_detected():
    c = Chain()
    snapshot = c.snap(10, [[70000, 5]], [])
    f11 = c.inc(11, [chg("bid", 69990, 3)])
    bad11 = copy.deepcopy(f11)
    bad11["changes"][0]["price"] = 69980
    with pytest.raises(DigestMismatchError) as ei:
        build_synced_book(snapshot, [bad11], SYMBOL)
    assert ei.value.seq == 11


def test_tampered_digest_field_detected():
    c = Chain()
    snapshot = c.snap(10, [[70000, 5]], [])
    f11 = c.inc(11, [chg("bid", 69990, 3)])
    bad11 = copy.deepcopy(f11)
    bad11["digest"] = ("0" if f11["digest"][0] != "0" else "1") + f11["digest"][1:]
    with pytest.raises(DigestMismatchError) as ei:
        build_synced_book(snapshot, [bad11], SYMBOL)
    assert ei.value.seq == 11


def test_frames_covered_by_snapshot_not_verified():
    """序号 <= 快照序号的缓冲帧不核验：即使摘要脏乱也正常丢弃。"""
    c = Chain()
    snapshot = c.snap(10, [[70000, 5]], [])
    dirty_old = {"type": "increment", "symbol": SYMBOL, "seq": 9,
                 "changes": [chg("bid", 1, 1)], "digest": "0" * 64}
    f11 = c.inc(11, [chg("bid", 69990, 3)])
    book, applied, dropped = build_synced_book(snapshot, [dirty_old, f11], SYMBOL)
    assert (applied, dropped) == (1, 1) and book.seq == 11
