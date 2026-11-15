"""测试辅助：按协议算法构造合法摘要链帧（模拟行情源行为）。"""
from app.digest import chain_digest, genesis_digest

SYMBOL = "CU2601"


def chg(side, price, qty):
    return {"side": side, "price": price, "qty": qty}


class Chain:
    """从 genesis 开始推进的合法帧构造器；snap() 携带当前链头作为核验基线。"""

    def __init__(self, symbol=SYMBOL):
        self.symbol = symbol
        self.digest = genesis_digest(symbol)

    def inc(self, seq, changes):
        msg = {"type": "increment", "symbol": self.symbol, "seq": seq, "changes": changes}
        self.digest = chain_digest(self.digest, self.symbol, seq, changes)
        msg["digest"] = self.digest
        return msg

    def snap(self, seq, bids, asks):
        return {
            "type": "snapshot",
            "symbol": self.symbol,
            "seq": seq,
            "bids": bids,
            "asks": asks,
            "digest": self.digest,
        }
