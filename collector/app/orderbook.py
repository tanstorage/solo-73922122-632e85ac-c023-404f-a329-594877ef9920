"""订单簿核心逻辑（纯函数/纯数据结构，不依赖 asyncio，便于单测）。

协议约定：
- 快照：{"type": "snapshot", "symbol": ..., "seq": N, "bids": [[价, 量], ...], "asks": [...],
         "digest": "..."}
  seq 为快照已包含的最后一条增量序号；digest 为该序号对应的摘要链头（核验基线）。
- 增量：{"type": "increment", "symbol": ..., "seq": N,
         "changes": [{"side", "price", "qty"}, ...], "digest": "..."}
  seq 全市场连续递增；qty == 0 表示删除该价位；同一增量内的变更原子可见；
  digest 为与前序关联的 SHA-256 链式摘要（见 app/digest.py）。
- 每侧最多 max_levels 档，价量均为整数。

核验范围：只核验序号晚于快照（或晚于已应用序号）的帧；
seq <= last 的重复帧不核验、直接跳过（正常重复投递不受影响）。
"""
from __future__ import annotations

from dataclasses import dataclass

from .digest import chain_digest, is_digest


class ProtocolError(Exception):
    """行情源消息违反协议（类型/取值/档数超限等）。"""


class GapError(Exception):
    """增量序号不连续（丢序）。"""


class StaleSnapshotError(Exception):
    """迟到快照：发起于上一次连接世代，必须丢弃。"""


class DigestMismatchError(Exception):
    """摘要断链或帧内容与摘要不符：传输途中价量被改写。seq 为首个异常序号。"""

    def __init__(self, seq: int, expected: str, actual: str) -> None:
        self.seq = seq
        self.expected = expected
        self.actual = actual
        super().__init__(f"digest mismatch at seq {seq}: expected {expected}, got {actual}")


def _is_int(value: object) -> bool:
    # bool 是 int 的子类，但 True/False 不是合法的价量
    return isinstance(value, int) and not isinstance(value, bool)


@dataclass(frozen=True)
class Change:
    side: str  # "bid" | "ask"
    price: int
    qty: int


def change_dicts(changes: list[Change]) -> list[dict]:
    """校验后的变更还原为协议字段字典，用于规范化串计算。"""
    return [{"side": c.side, "price": c.price, "qty": c.qty} for c in changes]


class OrderBook:
    """单合约订单簿。所有变更先校验、作用于副本再整体交换，保证原子可见。"""

    def __init__(self, symbol: str, max_levels: int = 20) -> None:
        self.symbol = symbol
        self.max_levels = max_levels
        self.bids: dict[int, int] = {}
        self.asks: dict[int, int] = {}
        self.seq: int | None = None     # 已应用的最后一条增量序号
        self.digest: str | None = None  # 已核验的摘要链头（对应 self.seq）

    def _check_levels(self, bids: dict[int, int], asks: dict[int, int]) -> None:
        if len(bids) > self.max_levels or len(asks) > self.max_levels:
            raise ProtocolError(
                f"levels exceed max_levels={self.max_levels}: "
                f"bids={len(bids)} asks={len(asks)}"
            )

    def load_snapshot(self, seq: int, bids: dict[int, int], asks: dict[int, int], digest: str) -> None:
        self._check_levels(bids, asks)
        self.bids = dict(bids)
        self.asks = dict(asks)
        self.seq = seq
        self.digest = digest

    def apply_increment(self, seq: int, changes: list[Change], digest: str) -> None:
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
        self.bids, self.asks, self.seq, self.digest = new_bids, new_asks, seq, digest

    def view(self) -> dict:
        """查询视图：序号 + 摘要链头 + 按价排序的双方盘口（买降序、卖升序）。"""
        return {
            "symbol": self.symbol,
            "seq": self.seq,
            "digest": self.digest,
            "bids": [[p, q] for p, q in sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)],
            "asks": [[p, q] for p, q in sorted(self.asks.items(), key=lambda kv: kv[0])],
        }


def validate_increment(msg: object, symbol: str) -> tuple[int, list[Change], str]:
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
    digest = msg.get("digest")
    if not is_digest(digest):
        raise ProtocolError(f"bad digest: {digest!r}")
    return seq, changes, digest


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


def validate_snapshot(data: object, symbol: str) -> tuple[int, dict[int, int], dict[int, int], str]:
    if not isinstance(data, dict):
        raise ProtocolError("snapshot is not an object")
    if data.get("symbol") != symbol:
        raise ProtocolError(f"unexpected symbol: {data.get('symbol')!r}")
    seq = data.get("seq")
    if not _is_int(seq) or seq < 0:
        raise ProtocolError(f"bad snapshot seq: {seq!r}")
    bids = _parse_levels(data.get("bids"), "bids")
    asks = _parse_levels(data.get("asks"), "asks")
    digest = data.get("digest")
    if not is_digest(digest):
        raise ProtocolError(f"bad snapshot digest: {digest!r}")
    return seq, bids, asks, digest


def build_synced_book(
    snapshot_data: object,
    buffered: list[object],
    symbol: str,
    max_levels: int = 20,
) -> tuple[OrderBook, int, int]:
    """用快照 + 缓冲增量构建已同步订单簿（含摘要链核验）。

    - 序号 <= 快照序号的缓冲增量（含重复）丢弃，不参与核验；
    - 之后的增量必须从 snapshot.seq + 1 开始逐条连续，恰好应用一次；
    - 每条被应用的增量都以快照 digest 为基线逐帧重算链式摘要并比对，
      断链/内容不符即抛 DigestMismatchError（首个异常序号），
      缺号即抛 GapError，均由上层触发整轮重同步。

    返回 (订单簿, 应用的增量条数, 丢弃的缓冲条数)。
    """
    snap_seq, bids, asks, snap_digest = validate_snapshot(snapshot_data, symbol)
    book = OrderBook(symbol=symbol, max_levels=max_levels)
    book.load_snapshot(snap_seq, bids, asks, snap_digest)

    last = snap_seq
    chain = snap_digest
    applied = 0
    dropped = 0
    for msg in buffered:
        seq, changes, digest = validate_increment(msg, symbol)
        if seq <= last:
            dropped += 1  # 快照已覆盖或重复投递，跳过保证"恰好应用一次"（不核验）
            continue
        if seq != last + 1:
            raise GapError(f"increment gap after snapshot: expected seq {last + 1}, got {seq}")
        expected = chain_digest(chain, symbol, seq, change_dicts(changes))
        if digest != expected:
            raise DigestMismatchError(seq, expected, digest)
        book.apply_increment(seq, changes, digest)
        chain = digest
        last = seq
        applied += 1
    return book, applied, dropped
