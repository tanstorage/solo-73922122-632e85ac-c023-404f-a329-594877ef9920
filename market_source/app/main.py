"""本地铜期货行情源（含故障注入，仅供开发/测试）。

- GET  /snapshot   全量快照：序号 + 完整买卖盘（可用 ?delay= 或控制面设置延迟）
- WS   /ws         增量流：连续序号、多档变更、qty=0 删档；另有周期心跳
- POST /control/*  故障注入：延迟快照、制造丢序、断线、重放重复增量、暂停推送
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("market-source")

SYMBOL = "CU2601"
MAX_LEVELS = 20
TICK = 10  # 最小价格变动，铜期货为 10 元/吨
MID_PRICE = 70000


class MarketSource:
    def __init__(self) -> None:
        self.symbol = SYMBOL
        self.seq = 0
        self.mid = MID_PRICE
        self.bids: dict[int, int] = {}
        self.asks: dict[int, int] = {}
        self.clients: set[WebSocket] = set()
        self.snapshot_delay = 0.0      # 快照响应延迟（秒），控制面可调
        self.pending_gap = 0           # 下一条增量要跳过的序号个数
        self.tick_interval = 0.5       # 增量推送间隔（秒）
        self.paused_until = 0.0        # 暂停推送直到该 monotonic 时刻
        self.last_increment: dict | None = None
        self._seed()

    def _seed(self) -> None:
        self.bids = {self.mid - TICK * i: 20 + 3 * i for i in range(1, 11)}
        self.asks = {self.mid + TICK * i: 15 + 4 * i for i in range(1, 11)}

    def snapshot(self) -> dict:
        return {
            "type": "snapshot",
            "symbol": self.symbol,
            "seq": self.seq,
            "bids": [[p, q] for p, q in sorted(self.bids.items(), reverse=True)],
            "asks": [[p, q] for p, q in sorted(self.asks.items())],
        }

    def make_increment(self) -> dict:
        # 丢序注入：序号一次性多跳 pending_gap 个，被跳过的序号永不发送
        self.seq += 1 + self.pending_gap
        self.pending_gap = 0

        changes = []
        for _ in range(random.randint(1, 3)):
            side = random.choice(("bid", "ask"))
            book = self.bids if side == "bid" else self.asks
            sign = -1 if side == "bid" else 1
            slots = [self.mid + sign * TICK * (i + 1) for i in range(MAX_LEVELS)]
            r = random.random()
            if book and r < 0.20:
                price, qty = random.choice(list(book)), 0      # 删档
            elif len(book) >= MAX_LEVELS:
                price = random.choice(list(book))              # 已满：改量
                qty = random.randint(5, 100)
            else:
                price = random.choice(slots)                   # 新增或改量
                qty = random.randint(5, 100)
            if qty == 0:
                book.pop(price, None)
            else:
                book[price] = qty
            changes.append({"side": side, "price": price, "qty": qty})

        inc = {"type": "increment", "symbol": self.symbol, "seq": self.seq, "changes": changes}
        self.last_increment = inc
        return inc


source = MarketSource()


async def broadcast(msg: dict) -> None:
    if not source.clients:
        return
    data = json.dumps(msg)
    dead = []
    for ws in list(source.clients):
        try:
            await ws.send_text(data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        source.clients.discard(ws)


async def tick_loop() -> None:
    while True:
        await asyncio.sleep(source.tick_interval)
        if time.monotonic() < source.paused_until:
            continue
        await broadcast(source.make_increment())


async def heartbeat_loop() -> None:
    while True:
        await asyncio.sleep(1.0)
        await broadcast({"type": "heartbeat", "ts": time.time()})


@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = [asyncio.create_task(tick_loop()), asyncio.create_task(heartbeat_loop())]
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()


app = FastAPI(
    title="Local Copper Market Source",
    version="1.0.0",
    description="本地行情源：快照 + 连续增量，支持延迟快照/丢序/断线等故障注入。",
    lifespan=lifespan,
)


@app.get("/", summary="导航")
async def index() -> dict:
    return {
        "service": app.title,
        "symbol": SYMBOL,
        "links": {"snapshot": "/snapshot", "ws": "/ws", "status": "/status", "docs": "/docs"},
    }


@app.get("/snapshot", summary="全量快照（序号 + 完整买卖盘）")
async def get_snapshot(
    symbol: str = SYMBOL,
    delay: float | None = Query(default=None, description="本次响应额外延迟秒数"),
):
    wait = source.snapshot_delay if delay is None else delay
    if wait > 0:
        await asyncio.sleep(wait)
    if symbol != source.symbol:
        return JSONResponse({"error": "unknown_symbol", "symbol": symbol}, status_code=404)
    return source.snapshot()


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    source.clients.add(ws)
    log.info("client connected, total=%d", len(source.clients))
    try:
        while True:
            await ws.receive_text()  # 忽略客户端消息，仅用于感知断开
    except WebSocketDisconnect:
        pass
    finally:
        source.clients.discard(ws)
        log.info("client disconnected, total=%d", len(source.clients))


@app.get("/status", summary="行情源状态")
async def get_status() -> dict:
    return {
        "symbol": source.symbol,
        "seq": source.seq,
        "levels": {"bids": len(source.bids), "asks": len(source.asks)},
        "clients": len(source.clients),
        "snapshot_delay": source.snapshot_delay,
        "pending_gap": source.pending_gap,
        "tick_interval": source.tick_interval,
        "paused": time.monotonic() < source.paused_until,
    }


# ---------------- 故障注入控制面 ----------------

@app.post("/control/gap", summary="制造丢序：下一条增量序号跳过 count 个")
async def control_gap(count: int = 1) -> dict:
    source.pending_gap = max(0, min(count, 1000))
    return {"ok": True, "pending_gap": source.pending_gap}


@app.post("/control/disconnect", summary="断线：关闭全部 WS 连接")
async def control_disconnect() -> dict:
    clients = list(source.clients)
    for ws in clients:
        try:
            await ws.close(code=1012)  # 1012 = Service Restart
        except Exception:
            pass
    return {"ok": True, "disconnected": len(clients)}


@app.post("/control/snapshot_delay", summary="设置快照响应延迟（秒，0 关闭）")
async def control_snapshot_delay(seconds: float = 5.0) -> dict:
    source.snapshot_delay = max(0.0, seconds)
    return {"ok": True, "snapshot_delay": source.snapshot_delay}


@app.post("/control/duplicate", summary="重放上一条增量（测试恰好应用一次）")
async def control_duplicate() -> dict:
    if source.last_increment is None:
        return {"ok": False, "reason": "no increment sent yet"}
    await broadcast(source.last_increment)
    return {"ok": True, "resent_seq": source.last_increment["seq"]}


@app.post("/control/pause", summary="暂停推送增量 seconds 秒（心跳继续）")
async def control_pause(seconds: float = 3.0) -> dict:
    source.paused_until = time.monotonic() + max(0.0, seconds)
    return {"ok": True, "paused_seconds": max(0.0, seconds)}
