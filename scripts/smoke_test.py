#!/usr/bin/env python3
"""端到端冒烟测试：验证盘口重建在丢序/断线/延迟快照下的行为。

用法：
    python3 scripts/smoke_test.py [--collector http://localhost:8001] [--source http://localhost:8000]

覆盖：
1. 启动后自动同步，/book 返回序号 + 排序盘口；
2. 重复增量恰好应用一次；
3. 丢序 -> /book 变 503 -> 自动重同步恢复；
4. 断线 -> 503 -> 自动恢复；
5. 延迟快照 + 断线 -> 恢复期间 503，最终恢复且序号不回退；
6. 全程序号单调不回退，盘口结构合法（整数价量、每侧 <=20 档、买卖不交叉）。
"""
from __future__ import annotations

import argparse
import sys
import time

import httpx

FAILURES: list[str] = []
last_seq_seen = -1


def check(name: str, cond: bool, detail: str = "") -> None:
    tag = "PASS" if cond else "FAIL"
    print(f"[{tag}] {name}" + (f"  ({detail})" if detail else ""))
    if not cond:
        FAILURES.append(name)


def check_book_shape(book: dict) -> None:
    global last_seq_seen
    seq = book["seq"]
    check("序号单调不回退", seq >= last_seq_seen, f"seq={seq} last={last_seq_seen}")
    last_seq_seen = max(last_seq_seen, seq)
    for side in ("bids", "asks"):
        levels = book[side]
        ok = (
            len(levels) <= 20
            and all(isinstance(p, int) and isinstance(q, int) and p > 0 and q > 0 for p, q in levels)
        )
        check(f"{side} 为整数价量且 <=20 档", ok)
    bids = [p for p, _ in book["bids"]]
    asks = [p for p, _ in book["asks"]]
    check("买盘按价降序", bids == sorted(bids, reverse=True))
    check("卖盘按价升序", asks == sorted(asks))
    if bids and asks:
        check("买卖不交叉", bids[0] < asks[0], f"bid={bids[0]} ask={asks[0]}")


def wait_synced(c: httpx.Client, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if c.get("/status").json().get("synced"):
                return True
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    return False


def wait_unsynced(c: httpx.Client, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if not c.get("/status").json().get("synced"):
                return True
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--collector", default="http://localhost:8001")
    ap.add_argument("--source", default="http://localhost:8000")
    args = ap.parse_args()

    col = httpx.Client(base_url=args.collector, timeout=10)
    src = httpx.Client(base_url=args.source, timeout=10)

    print("== 1. 启动后自动同步 ==")
    check("初始同步", wait_synced(col, 30))
    r = col.get("/book")
    check("/book 返回 200", r.status_code == 200)
    book = r.json()
    check_book_shape(book)
    stats0 = col.get("/status").json()["stats"]

    print("== 2. 重复增量恰好应用一次 ==")
    src.post("/control/duplicate")
    time.sleep(0.5)
    stats1 = col.get("/status").json()["stats"]
    check("重复被去重", stats1["duplicates_skipped"] > stats0["duplicates_skipped"],
          f"duplicates_skipped {stats0['duplicates_skipped']} -> {stats1['duplicates_skipped']}")
    check("重复后仍可用", col.get("/book").status_code == 200)

    print("== 3. 丢序 -> 503 -> 自动恢复 ==")
    src.post("/control/gap", params={"count": 1})
    check("缺号后进入失步", wait_unsynced(col, 10))
    r = col.get("/book")
    check("失步时 /book 返回 503", r.status_code == 503, f"got {r.status_code}")
    check("丢序后自动重同步", wait_synced(col, 30))
    check_book_shape(col.get("/book").json())
    stats2 = col.get("/status").json()["stats"]
    check("缺号被计数", stats2["gaps"] > stats1["gaps"])

    print("== 4. 断线 -> 503 -> 自动恢复 ==")
    src.post("/control/disconnect")
    check("断线后进入失步", wait_unsynced(col, 10))
    check("断线后自动重同步", wait_synced(col, 30))
    check_book_shape(col.get("/book").json())

    print("== 5. 延迟快照 + 断线（迟到快照不得覆盖新盘口） ==")
    src.post("/control/snapshot_delay", params={"seconds": 4})
    src.post("/control/disconnect")
    check("延迟快照期间进入失步", wait_unsynced(col, 10))
    r = col.get("/book")
    check("恢复期间 /book 返回 503", r.status_code == 503)
    # 快照在途时再次断线：旧快照响应迟到，必须被丢弃
    time.sleep(1.5)
    src.post("/control/disconnect")
    check("迟到快照场景最终恢复", wait_synced(col, 40))
    check_book_shape(col.get("/book").json())
    src.post("/control/snapshot_delay", params={"seconds": 0})
    st = col.get("/status").json()
    print(f"     stats: {st['stats']}")

    print("== 6. 连续观察 5 秒 ==")
    end = time.monotonic() + 5
    while time.monotonic() < end:
        r = col.get("/book")
        check("稳定期 /book 保持 200", r.status_code == 200)
        check_book_shape(r.json())
        time.sleep(0.7)

    print()
    if FAILURES:
        print(f"SMOKE FAILED: {len(FAILURES)} 项未过 -> {FAILURES}")
        return 1
    print("SMOKE OK: 全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
