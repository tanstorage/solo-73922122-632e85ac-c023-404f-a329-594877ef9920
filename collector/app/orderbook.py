"""订单簿核心逻辑（纯函数/纯数据结构，不依赖 asyncio，便于单测）。

协议约定：
- 快照：{"type": "snapshot", "symbol": ..., "seq": N, "bids": [[价, 量], ...], "asks": [...]}
  seq 为快照已包含的最后一条增量序号。
- 增量：{"type": "increment", "symbol": ..., "seq": N, "changes": [{"side", "price", "qty"}, ...]}
  seq 全市场连续递增；qty == 0 表示删除该价位；同一增量内的变更原子可见。
- 每侧最多 max_levels 档，价量均为整数。
"""
from __future__ import annotations

from dataclasses import dataclass


class ProtocolError(Exception):
    """行情源消息违反协议（类型/取值/档数超限等）。"""


class GapError(Exception):
    """增量序号不连续（丢序）。"""


class StaleSnapshotError(Exception):
    """迟到快照：发起于上一次连接世代，必须丢弃。"""


def _is_int(value: object) -> bool:
    # bool 是 int 的子类，但 True/False 不是合法的价量
    return isinstance(value, int) and not isinstance(value, bool)


@dataclass(frozen=True)
class Change:
    side: str  # "bid" | "ask"
    price: int
    qty: int


class OrderBook:
    """单合约订单簿。所有变更先校验、作用于副本再整体交换，保证原子可见。"""

    def __init__(self, symbol: str, max_levels: int = 20) -> None:
        self.symbol = symbol
        self.max_levels = max_levels
        self.bids: dict[int, int] = {}
        self.asks: dict[int, int] = {}
        self.seq: int | None = None  # 已应用的最后一条增量序号

    def _check_levels(self, bids: dict[int, int], asks: dict[int, int]) -> None:
        if len(bids) > self.max_levels or len(asks) > self.max_levels:
            raise ProtocolError(
                f"levels exceed max_levels={self.max_levels}: "
                f"bids={len(bids)} asks={len(asks)}"
            )

    def load_snapshot(self, seq: int, bids: dict[int, int], asks: dict[int, int]) -> None:
        self._check_levels(bids, asks)
        self.bids = dict(bids)
        self.asks = dict(asks)
        self.seq = seq

    def apply_increment(self, seq: int, changes: list[Change]) -> None:
        # 1) 全量校验，任何一条非法都不落簿
        for c in changes:
            if c.side not in ("bid", "ask"):
                raise ProtocolError(f"bad side: {c.side!r}")
            if not _is_int(c.price) or c.price <= 0:
                raise ProtocolError(f"bad price: {c.price!r}")
            if not _is_int(c.qty) or c.qty < 0:
                raise ProtocolError(f"bad qty: {c.qty!r}")
        # 2) 在副本上应用，然后整体交换 —— 同一条增量的变更同时可见
        new_bids = dict(self.bids)
        new_asks = dict(self.asks)
        for c in changes:
            side_book = new_bids if c.side == "bid" else new_asks
            if c.qty == 0:
                side_book.pop(c.price, None)  # 删档；不存在视为无操作
            else:
                side_book[c.price] = c.qty
        self._check_levels(new_bids, new_asks)
        self.bids, self.asks, self.seq = new_bids, new_asks, seq

    def view(self) -> dict:
        """查询视图：序号 + 按价排序的双方盘口（买降序、卖升序）。"""
        return {
            "symbol": self.symbol,
            "seq": self.seq,
            "bids": [[p, q] for p, q in sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)],
            "asks": [[p, q] for p, q in sorted(self.asks.items(), key=lambda kv: kv[0])],
        }


def validate_increment(msg: object, symbol: str) -> tuple[int, list[Change]]:
    if not isinstance(msg, dict):
        raise ProtocolError("increment frame is not an object")
    if msg.get("type") != "increment":
        raise ProtocolError(f"unexpected frame type: {msg.get('type')!r}")
    if msg.get("symbol") != symbol:
        raise ProtocolError(f"unexpected symbol: {msg.get('symbol')!r}")
    seq = msg.get("seq")
    if not _is_int(seq) or seq < 0:
        raise ProtocolError(f"bad seq: {seq!r}")
    raw_changes = msg.get("changes")
    if not isinstance(raw_changes, list) or not raw_changes:
        raise ProtocolError("changes must be a non-empty list")
    changes: list[Change] = []
    for item in raw_changes:
        if not isinstance(item, dict):
            raise ProtocolError(f"change is not an object: {item!r}")
        changes.append(Change(side=item.get("side"), price=item.get("price"), qty=item.get("qty")))
    return seq, changes


def _parse_levels(raw: object, name: str) -> dict[int, int]:
    if not isinstance(raw, list):
        raise ProtocolError(f"{name} must be a list")
    out: dict[int, int] = {}
    for entry in raw:
        if not (isinstance(entry, (list, tuple)) and len(entry) == 2):
            raise ProtocolError(f"bad {name} entry: {entry!r}")
        price, qty = entry
        if not _is_int(price) or price <= 0:
            raise ProtocolError(f"bad price in {name}: {price!r}")
        if not _is_int(qty) or qty <= 0:
            raise ProtocolError(f"bad qty in {name}: {qty!r}")
        if price in out:
            raise ProtocolError(f"duplicate price in {name}: {price}")
        out[price] = qty
    return out


def validate_snapshot(data: object, symbol: str) -> tuple[int, dict[int, int], dict[int, int]]:
    if not isinstance(data, dict):
        raise ProtocolError("snapshot is not an object")
    if data.get("symbol") != symbol:
        raise ProtocolError(f"unexpected symbol: {data.get('symbol')!r}")
    seq = data.get("seq")
    if not _is_int(seq) or seq < 0:
        raise ProtocolError(f"bad snapshot seq: {seq!r}")
    bids = _parse_levels(data.get("bids"), "bids")
    asks = _parse_levels(data.get("asks"), "asks")
    return seq, bids, asks


def build_synced_book(
    snapshot_data: object,
    buffered: list[object],
    symbol: str,
    max_levels: int = 20,
) -> tuple[OrderBook, int, int]:
    """用快照 + 缓冲增量构建已同步订单簿。

    - 序号 <= 快照序号的缓冲增量（含重复）丢弃；
    - 之后的增量必须从 snapshot.seq + 1 开始逐条连续，恰好应用一次；
    - 任一环节缺号即抛 GapError，由上层触发整轮重同步。

    返回 (订单簿, 应用的增量条数, 丢弃的缓冲条数)。
    """
    snap_seq, bids, asks = validate_snapshot(snapshot_data, symbol)
    book = OrderBook(symbol=symbol, max_levels=max_levels)
    book.load_snapshot(snap_seq, bids, asks)

    last = snap_seq
    applied = 0
    dropped = 0
    for msg in buffered:
        seq, changes = validate_increment(msg, symbol)
        if seq <= last:
            dropped += 1  # 快照已覆盖或重复投递，跳过保证“恰好应用一次”
            continue
        if seq != last + 1:
            raise GapError(f"increment gap after snapshot: expected seq {last + 1}, got {seq}")
        book.apply_increment(seq, changes)
        last = seq
        applied += 1
    return book, applied, dropped
