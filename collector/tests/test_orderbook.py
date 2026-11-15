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

SYMBOL = "CU2601"


def inc(seq, changes):
    return {"type": "increment", "symbol": SYMBOL, "seq": seq, "changes": changes}


def chg(side, price, qty):
    return {"side": side, "price": price, "qty": qty}


def snap(seq, bids, asks):
    return {"type": "snapshot", "symbol": SYMBOL, "seq": seq, "bids": bids, "asks": asks}


# ---------- OrderBook ----------

def test_apply_add_update_delete():
    book = OrderBook(SYMBOL)
    book.load_snapshot(10, {69990: 5}, {70010: 7})
    book.apply_increment(11, [Change("bid", 69980, 3)])      # 新增
    book.apply_increment(12, [Change("bid", 69990, 9)])      # 改量
    book.apply_increment(13, [Change("ask", 70010, 0)])      # 删档
    assert book.bids == {69990: 9, 69980: 3}
    assert book.asks == {}
    assert book.seq == 13


def test_delete_nonexistent_is_noop():
    book = OrderBook(SYMBOL)
    book.load_snapshot(1, {}, {})
    book.apply_increment(2, [Change("bid", 12345, 0)])
    assert book.bids == {}


def test_invalid_increment_is_atomic():
    book = OrderBook(SYMBOL)
    book.load_snapshot(1, {100: 1}, {})
    with pytest.raises(ProtocolError):
        book.apply_increment(2, [Change("bid", 100, 5), Change("bid", -1, 3)])
    assert book.bids == {100: 1} and book.seq == 1  # 一条非法，整条不落簿


def test_bool_and_float_rejected():
    book = OrderBook(SYMBOL)
    book.load_snapshot(1, {}, {})
    for bad in [Change("bid", True, 3), Change("bid", 100, 1.5)]:
        with pytest.raises(ProtocolError):
            book.apply_increment(2, [bad])


def test_max_levels_enforced():
    book = OrderBook(SYMBOL, max_levels=2)
    book.load_snapshot(1, {100: 1, 90: 1}, {})
    with pytest.raises(ProtocolError):
        book.apply_increment(2, [Change("bid", 80, 1)])  # 第 3 档超限
    assert len(book.bids) == 2


def test_view_sorted():
    book = OrderBook(SYMBOL)
    book.load_snapshot(5, {90: 1, 100: 2, 80: 3}, {110: 4, 105: 5})
    view = book.view()
    assert view["seq"] == 5
    assert view["bids"] == [[100, 2], [90, 1], [80, 3]]   # 买降序
    assert view["asks"] == [[105, 5], [110, 4]]           # 卖升序


# ---------- 快照 + 缓冲增量衔接 ----------

def test_build_applies_only_after_snapshot_seq():
    snapshot = snap(10, [[100, 1]], [[200, 2]])
    buffered = [inc(9, [chg("bid", 100, 9)]),   # 快照已覆盖 -> 丢弃
                inc(11, [chg("bid", 90, 5)]),
                inc(12, [chg("ask", 200, 0)])]
    book, applied, dropped = build_synced_book(snapshot, buffered, SYMBOL)
    assert book.seq == 12
    assert book.bids == {100: 1, 90: 5}
    assert book.asks == {}
    assert (applied, dropped) == (2, 1)


def test_build_duplicate_in_buffer_applied_once():
    snapshot = snap(10, [[100, 1]], [])
    buffered = [inc(11, [chg("bid", 100, 7)]), inc(11, [chg("bid", 100, 7)])]
    book, applied, dropped = build_synced_book(snapshot, buffered, SYMBOL)
    assert book.bids == {100: 7} and book.seq == 11
    assert (applied, dropped) == (1, 1)


def test_build_gap_raises():
    snapshot = snap(10, [[100, 1]], [])
    buffered = [inc(12, [chg("bid", 90, 5)])]  # 缺 11
    with pytest.raises(GapError):
        build_synced_book(snapshot, buffered, SYMBOL)


def test_build_empty_buffer_ok():
    book, applied, dropped = build_synced_book(snap(7, [[1, 1]], []), [], SYMBOL)
    assert book.seq == 7 and (applied, dropped) == (0, 0)


# ---------- 协议校验 ----------

def test_validate_increment_rejects_bad_frames():
    with pytest.raises(ProtocolError):
        validate_increment({"type": "increment", "symbol": "XX", "seq": 1, "changes": [chg("bid", 1, 1)]}, SYMBOL)
    with pytest.raises(ProtocolError):
        validate_increment(inc(1, []), SYMBOL)  # 空变更
    with pytest.raises(ProtocolError):
        validate_increment(inc("1", [chg("bid", 1, 1)]), SYMBOL)


def test_validate_snapshot_rejects_bad_data():
    with pytest.raises(ProtocolError):
        validate_snapshot(snap(1, [[100, 1], [100, 2]], []), SYMBOL)  # 重复价位
    with pytest.raises(ProtocolError):
        validate_snapshot(snap(1, [[100, 0]], []), SYMBOL)             # 快照量必须为正
    with pytest.raises(ProtocolError):
        validate_snapshot(snap(-1, [], []), SYMBOL)
