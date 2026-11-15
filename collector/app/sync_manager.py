"""同步管理器：WS 接收 + 快照衔接 + 摘要链核验 + 断线/丢序/篡改自愈。

状态机：
- 启动 / 断流 / 发现缺号 / 摘要核验失败 -> synced=False（查询返回 503），进入新一轮恢复；
- 每轮恢复：先连 WS 缓冲增量，同时取快照；快照返回后用缓冲中
  seq > snapshot.seq 的增量连续补齐（恰好一次），全部衔接才 synced=True；
- 核验范围：只核验序号晚于快照的缓冲帧及此后的实时帧；重复帧不核验直接跳过；
- 摘要断链或帧内容不符 -> 记录首个异常序号（/status.verification.anomaly_seq），
  /book 立即 503，随后按原流程重建；
- 每次重连递增 generation，重连前发起的迟到快照响应一律丢弃，
  旧连接迟到的帧/核验 likewise 按世代隔离，不得使新连接失步或误报异常。
"""
from __future__ import annotations

import asyncio
import json
import logging

import httpx
import websockets

from .config import Settings
from .digest import ALGO, chain_digest
from .orderbook import (
    DigestMismatchError,
    GapError,
    OrderBook,
    ProtocolError,
    StaleSnapshotError,
    build_synced_book,
    change_dicts,
    validate_increment,
)

log = logging.getLogger("collector.sync")

_CLOSED = object()  # 队列哨兵：WS 接收循环结束


class SyncManager:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.book = OrderBook(symbol=settings.symbol, max_levels=settings.max_levels)
        self.synced = False
        self.generation = 0  # 连接世代号，每轮恢复 +1
        self.lock = asyncio.Lock()  # 保护 book/synced，读写两侧都走它
        self.last_error: str | None = None
        # 当前未恢复episode的首个异常（核验失败）；恢复后清空，last_anomaly 永久留痕
        self.anomaly: dict | None = None
        self.last_anomaly: dict | None = None
        self.stats: dict[str, int] = {
            "increments_applied": 0,
            "increments_verified": 0,
            "duplicates_skipped": 0,
            "snapshots_applied": 0,
            "stale_snapshots": 0,
            "stale_frames": 0,
            "buffered_applied": 0,
            "buffered_dropped": 0,
            "gaps": 0,
            "verification_failures": 0,
            "resyncs": 0,
        }
        self._task: asyncio.Task | None = None
        self._http: httpx.AsyncClient | None = None

    # ---------- 生命周期 ----------

    async def start(self) -> None:
        self._http = httpx.AsyncClient(
            base_url=self.settings.source_http,
            timeout=self.settings.snapshot_timeout,
        )
        self._task = asyncio.create_task(self._run(), name="sync-manager")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._http is not None:
            await self._http.aclose()

    # ---------- 主循环 ----------

    async def _run(self) -> None:
        while True:
            self.generation += 1
            gen = self.generation
            async with self.lock:
                self.synced = False
            try:
                await self._cycle(gen)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 断线/缺号/协议错误/HTTP 错误都走重同步
                self.last_error = f"{type(exc).__name__}: {exc}"
                self.stats["resyncs"] += 1
                log.warning("sync cycle failed (gen=%d): %s", gen, self.last_error)
            finally:
                async with self.lock:
                    self.synced = False
            await asyncio.sleep(self.settings.reconnect_delay)

    async def _cycle(self, gen: int) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        closed = asyncio.Event()
        recv_state: dict[str, object] = {"error": None}

        async def receiver(ws) -> None:
            try:
                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=self.settings.data_timeout)
                    try:
                        msg = json.loads(raw)
                    except (json.JSONDecodeError, TypeError) as exc:
                        raise ProtocolError(f"invalid JSON frame: {exc}")
                    if not isinstance(msg, dict):
                        raise ProtocolError("frame is not an object")
                    mtype = msg.get("type")
                    if mtype == "increment":
                        queue.put_nowait(msg)  # 取快照期间也持续缓冲，一条不丢
                    elif mtype == "heartbeat":
                        continue  # 仅用于保活/断流检测
                    else:
                        raise ProtocolError(f"unknown frame type: {mtype!r}")
            except Exception as exc:
                recv_state["error"] = exc
            finally:
                closed.set()
                queue.put_nowait(_CLOSED)

        async with websockets.connect(
            self.settings.source_ws, ping_interval=20, ping_timeout=20, max_size=1 << 20
        ) as ws:
            recv_task = asyncio.create_task(receiver(ws))
            snap_task = asyncio.create_task(self._fetch_snapshot(gen))
            try:
                # 等快照，同时接收循环持续缓冲增量；连接先断则本轮作废
                closed_waiter = asyncio.create_task(closed.wait())
                await asyncio.wait({snap_task, closed_waiter}, return_when=asyncio.FIRST_COMPLETED)
                closed_waiter.cancel()
                if not snap_task.done():
                    snap_task.cancel()
                    raise ConnectionError("stream closed before snapshot arrived")
                snapshot = snap_task.result()  # HTTP/协议错误在此抛出
                if snapshot is None:
                    raise StaleSnapshotError(f"snapshot of generation {gen} discarded")

                # 排干快照在途期间缓冲的增量
                buffered: list[dict] = []
                while True:
                    try:
                        item = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if item is _CLOSED:
                        raise ConnectionError("stream closed while applying snapshot")
                    buffered.append(item)

                await self._install_snapshot(snapshot, buffered, gen)

                # 已同步：逐条衔接后续增量
                while True:
                    item = await queue.get()
                    if item is _CLOSED:
                        err = recv_state["error"]
                        raise ConnectionError(f"stream closed: {err}" if err else "stream closed by peer")
                    await self._apply_increment(item, gen)
            finally:
                recv_task.cancel()
                snap_task.cancel()
                try:
                    await asyncio.gather(recv_task, snap_task, return_exceptions=True)
                except asyncio.CancelledError:
                    pass

    # ---------- 快照 ----------

    async def _fetch_snapshot(self, gen: int) -> dict | None:
        """取快照；返回时若世代已更替（重连前的迟到响应）则丢弃。"""
        assert self._http is not None
        resp = await self._http.get("/snapshot", params={"symbol": self.settings.symbol})
        resp.raise_for_status()
        data = resp.json()
        if gen != self.generation:
            self.stats["stale_snapshots"] += 1
            log.warning("discarding stale snapshot (gen=%d, current=%d)", gen, self.generation)
            return None
        return data

    async def _install_snapshot(self, snapshot: dict, buffered: list[dict], gen: int) -> None:
        try:
            book, applied, dropped = build_synced_book(
                snapshot, buffered, symbol=self.settings.symbol, max_levels=self.settings.max_levels
            )
        except DigestMismatchError as exc:
            async with self.lock:
                # 仅当前世代的核验失败才记录；旧世代迟到核验不得误报新连接
                if gen == self.generation:
                    self._record_anomaly(exc.seq, f"buffered frame rejected: {exc}", gen)
            raise
        async with self.lock:
            if gen != self.generation:
                # 双重保险：安装前再核一次世代，迟到快照绝不覆盖新盘口
                self.stats["stale_snapshots"] += 1
                raise StaleSnapshotError(
                    f"refusing to install snapshot of generation {gen} (current {self.generation})"
                )
            self.book = book
            self.synced = True
            self.anomaly = None  # 新基线建立，当前异常episode结束（last_anomaly 留痕）
            self.stats["snapshots_applied"] += 1
            self.stats["buffered_applied"] += applied
            self.stats["buffered_dropped"] += dropped
            self.stats["increments_verified"] += applied
            self.last_error = None
        log.info("snapshot installed: seq=%s applied=%d dropped=%d", book.seq, applied, dropped)

    # ---------- 增量 ----------

    def _record_anomaly(self, seq: int, reason: str, gen: int) -> None:
        """记录首个异常序号（调用方须持锁）。当前episode与留痕同时更新。"""
        entry = {"seq": seq, "reason": reason, "generation": gen}
        self.anomaly = entry
        self.last_anomaly = entry
        self.stats["verification_failures"] += 1
        log.error("digest chain anomaly: seq=%s gen=%d: %s", seq, gen, reason)

    async def _apply_increment(self, msg: dict, gen: int) -> None:
        seq, changes, digest = validate_increment(msg, self.settings.symbol)
        async with self.lock:
            if gen != self.generation:
                # 旧连接迟到的帧：不核验、不应用，绝不得使新连接失步
                self.stats["stale_frames"] += 1
                return
            if not self.synced or self.book.seq is None or self.book.digest is None:
                raise ProtocolError("increment received while unsynced")
            last = self.book.seq
            if seq <= last:
                self.stats["duplicates_skipped"] += 1
                return  # 重复投递：恰好应用一次（不重复核验）
            if seq != last + 1:
                self.stats["gaps"] += 1
                raise GapError(f"increment gap: expected seq {last + 1}, got {seq}")
            expected = chain_digest(self.book.digest, self.settings.symbol, seq, change_dicts(changes))
            if digest != expected:
                self._record_anomaly(seq, f"expected {expected}, got {digest}", gen)
                raise DigestMismatchError(seq, expected, digest)
            self.book.apply_increment(seq, changes, digest)
            self.stats["increments_applied"] += 1
            self.stats["increments_verified"] += 1

    # ---------- 查询视图 ----------

    def status_view(self) -> dict:
        s = self.settings
        return {
            "symbol": s.symbol,
            "synced": self.synced,
            "seq": self.book.seq,
            "generation": self.generation,
            "levels": {"bids": len(self.book.bids), "asks": len(self.book.asks)},
            "last_error": self.last_error,
            "verification": {
                "algo": ALGO,
                "chain_ok": self.synced and self.anomaly is None,
                "chain_head": self.book.digest,
                "verified_increments": self.stats["increments_verified"],
                "failures": self.stats["verification_failures"],
                "anomaly_seq": self.anomaly["seq"] if self.anomaly else None,
                "anomaly_reason": self.anomaly["reason"] if self.anomaly else None,
                "last_anomaly": self.last_anomaly,
            },
            "stats": dict(self.stats),
            "source": {"http": s.source_http, "ws": s.source_ws},
            "config": {
                "max_levels": s.max_levels,
                "reconnect_delay": s.reconnect_delay,
                "data_timeout": s.data_timeout,
                "snapshot_timeout": s.snapshot_timeout,
            },
        }
