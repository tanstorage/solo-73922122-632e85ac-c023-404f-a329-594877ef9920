"""订单簿与快照衔接逻辑单元测试。"""
import pytest

from app.orderbook import (
    Change,
    GapError,
    OrderBook,
    ProtocolError,
    build_synced_book,
    validate_increment,
    validate_snapshot,
)
from helpers import SYMBOL, Chain, chg

D = "a" * 64  # 测试用合法格式摘要（不涉及链核验的场景）


def inc(seq, changes, digest=D):
    return {"type": "increment", "symbol": SYMBOL, "seq": seq, "changes": changes, "digest": digest}


def snap(seq, bids, asks, digest=D):
    return {"type": "snapshot", "symbol": SYMBOL, "seq": seq, "bids": bids, "asks": asks, "digest": digest}


# ---------- OrderBook ----------

def test_apply_add_update_delete():
    book = OrderBook(SYMBOL)
    book.load_snapshot(10, {69990: 5}, {70010: 7}, D)
    book.apply_increment(11, [Change("bid", 69980, 3)], D)      # 新增
    book.apply_increment(12, [Change("bid", 69990, 9)], D)      # 改量
    book.apply_increment(13, [Change("ask", 70010, 0)], D)      # 删档
    assert book.bids == {69990: 9, 69980: 3}
    assert book.asks == {}
    assert book.seq == 13


def test_delete_nonexistent_is_noop():
    book = OrderBook(SYMBOL)
    book.load_snapshot(1, {}, {}, D)
    book.apply_increment(2, [Change("bid", 12345, 0)], D)
    assert book.bids == {}


def test_invalid_increment_is_atomic():
    book = OrderBook(SYMBOL)
    book.load_snapshot(1, {100: 1}, {}, D)
    with pytest.raises(ProtocolError):
        book.apply_increment(2, [Change("bid", 100, 5), Change("bid", -1, 3)], D)
    assert book.bids == {100: 1} and book.seq == 1  # 一条非法，整条不落簿


def test_bool_and_float_rejected():
    book = OrderBook(SYMBOL)
    book.load_snapshot(1, {}, {}, D)
    for bad in [Change("bid", True, 3), Change("bid", 100, 1.5)]:
        with pytest.raises(ProtocolError):
            book.apply_increment(2, [bad], D)


def test_max_levels_enforced():
    book = OrderBook(SYMBOL, max_levels=2)
    book.load_snapshot(1, {100: 1, 90: 1}, {}, D)
    with pytest.raises(ProtocolError):
        book.apply_increment(2, [Change("bid", 80, 1)], D)  # 第 3 档超限
    assert len(book.bids) == 2


def test_view_sorted():
    book = OrderBook(SYMBOL)
    book.load_snapshot(5, {90: 1, 100: 2, 80: 3}, {110: 4, 105: 5}, D)
    view = book.view()
    assert view["seq"] == 5
    assert view["digest"] == D
    assert view["bids"] == [[100, 2], [90, 1], [80, 3]]   # 买降序
    assert view["asks"] == [[105, 5], [110, 4]]           # 卖升序


# ---------- 快照 + 缓冲增量衔接 ----------

def test_build_applies_only_after_snapshot_seq():
    c = Chain()
    snapshot = c.snap(10, [[100, 1]], [[200, 2]])
    buffered = [inc(9, [chg("bid", 100, 9)]),   # 快照已覆盖 -> 丢弃（不核验）
                c.inc(11, [chg("bid", 90, 5)]),
                c.inc(12, [chg("ask", 200, 0)])]
    book, applied, dropped = build_synced_book(snapshot, buffered, SYMBOL)
    assert book.seq == 12
    assert book.bids == {100: 1, 90: 5}
    assert book.asks == {}
    assert book.digest == c.digest  # 链头推进到 seq 12
    assert (applied, dropped) == (2, 1)


def test_build_duplicate_in_buffer_applied_once():
    c = Chain()
    snapshot = c.snap(10, [[100, 1]], [])
    f11 = c.inc(11, [chg("bid", 100, 7)])
    book, applied, dropped = build_synced_book(snapshot, [f11, dict(f11)], SYMBOL)
    assert book.bids == {100: 7} and book.seq == 11
    assert (applied, dropped) == (1, 1)


def test_build_gap_raises():
    c = Chain()
    snapshot = c.snap(10, [[100, 1]], [])
    with pytest.raises(GapError):
        build_synced_book(snapshot, [c.inc(12, [chg("bid", 90, 5)])], SYMBOL)  # 缺 11


def test_build_empty_buffer_ok():
    c = Chain()
    book, applied, dropped = build_synced_book(c.snap(7, [[1, 1]], []), [], SYMBOL)
    assert book.seq == 7 and (applied, dropped) == (0, 0)


# ---------- 协议校验 ----------

def test_validate_increment_rejects_bad_frames():
    with pytest.raises(ProtocolError):
        validate_increment({"type": "increment", "symbol": "XX", "seq": 1,
                            "changes": [chg("bid", 1, 1)], "digest": D}, SYMBOL)
    with pytest.raises(ProtocolError):
        validate_increment(inc(1, []), SYMBOL)  # 空变更
    with pytest.raises(ProtocolError):
        validate_increment(inc("1", [chg("bid", 1, 1)]), SYMBOL)


def test_validate_increment_requires_digest():
    no_digest = {"type": "increment", "symbol": SYMBOL, "seq": 1, "changes": [chg("bid", 1, 1)]}
    with pytest.raises(ProtocolError):
        validate_increment(no_digest, SYMBOL)                       # 缺 digest
    with pytest.raises(ProtocolError):
        validate_increment(inc(1, [chg("bid", 1, 1)], digest="abc"), SYMBOL)  # 长度不符
    with pytest.raises(ProtocolError):
        validate_increment(inc(1, [chg("bid", 1, 1)], digest="A" * 64), SYMBOL)  # 须小写 hex
    seq, changes, digest = validate_increment(inc(1, [chg("bid", 1, 1)]), SYMBOL)
    assert (seq, digest) == (1, D) and changes[0].price == 1


def test_validate_snapshot_rejects_bad_data():
    with pytest.raises(ProtocolError):
        validate_snapshot(snap(1, [[100, 1], [100, 2]], []), SYMBOL)  # 重复价位
    with pytest.raises(ProtocolError):
        validate_snapshot(snap(1, [[100, 0]], []), SYMBOL)             # 快照量必须为正
    with pytest.raises(ProtocolError):
        validate_snapshot(snap(-1, [], []), SYMBOL)


def test_validate_snapshot_requires_digest():
    data = snap(1, [[100, 1]], [])
    del data["digest"]
    with pytest.raises(ProtocolError):
        validate_snapshot(data, SYMBOL)
    with pytest.raises(ProtocolError):
        validate_snapshot(snap(1, [[100, 1]], [], digest="zz"), SYMBOL)
