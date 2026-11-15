"""SHA-256 摘要链（传输完整性核验，算法即协议）。

行情源与采集器各自持有本模块的相同副本（服务独立部署，不共享代码包），
两端 ALGO 版本一致才能互通。

链式规则（digest 均为 64 位小写十六进制串）：

    digest_0 = sha256(f"{symbol}|genesis")
    digest_N = sha256(f"{digest_{N-1}}|{canonical_increment(symbol, N, changes_N)}")

- 每条增量帧携带 digest_N；快照携带其 seq 对应的链头 digest，作为核验基线。
- 摘要与前序关联：任一帧的价量在传输途中被改写，该帧重算结果即与携带值不符，
  且其后所有帧连环失配 —— 即使后续改写"抵消"了盘口差异，首个异常序号仍被定位。
"""
from __future__ import annotations

import hashlib
import json

ALGO = "sha256-chain-v1"
_HEX = frozenset("0123456789abcdef")


def genesis_digest(symbol: str) -> str:
    """链起点：seq=0 时的摘要（seq=0 的快照携带此值）。"""
    return hashlib.sha256(f"{symbol}|genesis".encode("utf-8")).hexdigest()


def canonical_increment(symbol: str, seq: int, changes: list[dict]) -> str:
    """增量帧的规范化串：只含 symbol/seq/changes(side,price,qty)，键序固定、无空白。

    两端对同一逻辑帧必须得到逐字节相同的串，因此只取协议字段、
    排序键名、紧凑分隔符；帧上任何额外字段不参与摘要。
    """
    payload = {
        "symbol": symbol,
        "seq": seq,
        "changes": [
            {"side": c["side"], "price": c["price"], "qty": c["qty"]} for c in changes
        ],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def chain_digest(prev_digest: str, symbol: str, seq: int, changes: list[dict]) -> str:
    """由前序摘要与帧内容推出本条摘要。"""
    body = canonical_increment(symbol, seq, changes)
    return hashlib.sha256(f"{prev_digest}|{body}".encode("utf-8")).hexdigest()


def is_digest(value: object) -> bool:
    """是否为合法的摘要串（64 位小写十六进制）。"""
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(ch in _HEX for ch in value)
    )
