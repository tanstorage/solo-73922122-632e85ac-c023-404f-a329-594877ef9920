"""采集器配置，全部可用环境变量覆盖。"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Settings:
    symbol: str = os.getenv("SYMBOL", "CU2601")
    source_http: str = os.getenv("SOURCE_HTTP", "http://localhost:8000")
    source_ws: str = os.getenv("SOURCE_WS", "ws://localhost:8000/ws")
    max_levels: int = int(os.getenv("MAX_LEVELS", "20"))
    reconnect_delay: float = float(os.getenv("RECONNECT_DELAY", "1.0"))
    # 超过该时间收不到任何帧（含心跳）视为断流
    data_timeout: float = float(os.getenv("DATA_TIMEOUT", "5.0"))
    # 快照 HTTP 请求超时（需大于行情源可能注入的快照延迟）
    snapshot_timeout: float = float(os.getenv("SNAPSHOT_TIMEOUT", "30.0"))
