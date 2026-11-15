# 铜期货盘口重建服务（CU2601）

行情断线/丢序/传输篡改后的可信盘口重建环境：本地行情源 + 采集器，FastAPI + httpx + websockets。
单合约、每侧最多 20 档、整数价量；增量逐帧携带 SHA-256 链式摘要，采集器逐帧核验，
传输途中被改写的价量可按**首个异常序号**定位。

## 快速启动

```bash
docker compose up --build
```

| 服务 | 地址 | 说明 |
|---|---|---|
| 采集器 collector | http://localhost:8001/docs | `/book` 查盘口、`/status` 查同步与核验状态、`/verification` 核验专视图 |
| 行情源 market-source | http://localhost:8000/docs | `/snapshot`、`/ws`、`/control/*` 故障注入（含传输篡改） |

不开 Docker 时本地直跑：

```bash
pip install -r market_source/requirements.txt -r collector/requirements-dev.txt
(cd market_source && uvicorn app.main:app --port 8000 &)
(cd collector && SOURCE_WS=ws://localhost:8000/ws uvicorn app.main:app --port 8001 &)
python3 scripts/smoke_test.py     # 端到端冒烟
(cd collector && python3 -m pytest tests/ -q)   # 单元测试
```

## 数据协议

- **HTTP 快照** `GET /snapshot`：`{symbol, seq, bids: [[价,量]...], asks: [...], digest}`，
  `seq` 为快照已包含的最后一条增量序号；`digest` 为该序号对应的**摘要链头**（核验基线）。
- **WS 增量** `/ws`：`{type:"increment", symbol, seq, changes:[{side,price,qty}...], digest}`，
  `seq` 全市场连续递增；一条增量可含多档变更；`qty=0` 表示删档；另有心跳帧保活。
- **摘要链**（`sha256-chain-v1`，两端各自内置同一算法，见 `*/app/digest.py`）：
  ```
  digest_0 = sha256(f"{symbol}|genesis")
  digest_N = sha256(f"{digest_{N-1}}|{canonical(increment_N)}")
  canonical = json.dumps({symbol, seq, changes:[{side,price,qty}...]},
                         sort_keys=True, separators=(",", ":"))
  ```
  任一帧价量在传输途中被改写，该帧重算结果即与携带的 digest 不符，且其后所有帧
  连环失配 —— 即使后续改写"抵消"了盘口差异，首个异常序号仍被定位。

## 采集器衔接与核验规则（collector/app/sync_manager.py）

1. **启动 / 断流 / 发现缺号 / 摘要核验失败 → 立即失步**：`/book` 返回 503（携带
   `anomaly_seq`），`/status.synced=false`，`/status.verification.anomaly_seq` 标明首个异常序号。
2. **恢复流程**：先连 WS 并**持续缓冲增量**（取快照期间一条不丢），同时用 httpx 取快照。
3. **衔接**：丢弃 `seq ≤ 快照seq` 的缓冲（含重复，保证**恰好应用一次**，不参与核验），
   其余必须从 `快照seq+1` 开始**逐条连续**，否则本轮作废重来。
4. **核验范围**：只核验序号晚于快照的缓冲帧及此后的实时帧 —— 以快照 `digest` 为基线
   逐帧重算链式摘要并比对帧携带值；**断链或内容不符即记首个异常序号**（
   `verification.anomaly_seq` + `last_anomaly` 留痕），随后按原流程重建。
5. **完整衔接才恢复查询**；同步后每条增量仍校验 `seq == last+1` 且摘要匹配，
   缺号或失配即回到第 1 步。重复投递（`seq ≤ last`）不核验、直接跳过。
6. **迟到防护（世代隔离）**：每轮恢复递增 `generation`；重连前发起的迟到快照响应
   一律丢弃（`stale_snapshots` 计数），旧连接迟到的帧 likewise 丢弃（`stale_frames` 计数）
   —— **旧连接的迟到核验不得使新连接失步或误报异常**。
7. **原子可见**：同一增量的多档变更先在副本上应用再整体交换，
   查询只能看到应用前或应用后的完整状态；`/book` 返回 `seq` + `digest` + 买降序/卖升序盘口。

## 故障注入（行情源控制面，均可在 http://localhost:8000/docs 上直接调用）

```bash
curl -X POST 'http://localhost:8000/control/gap?count=1'          # 丢序：跳过 1 个序号
curl -X POST 'http://localhost:8000/control/disconnect'           # 断线：关闭全部 WS
curl -X POST 'http://localhost:8000/control/snapshot_delay?seconds=5'  # 快照延迟 5s
curl -X POST 'http://localhost:8000/control/duplicate'            # 重放上一条增量（测去重）
curl -X POST 'http://localhost:8000/control/pause?seconds=3'      # 暂停推送（心跳继续）
curl -X POST 'http://localhost:8000/control/tamper?field=qty'     # 传输篡改：qty|price|digest
```

**传输篡改**（质控演练核心）：立即发出一条增量，**只改发出帧，不改源盘口和摘要链**
（源侧按原始内容正常推进，模拟链路上的改写）。响应返回 `tampered_seq` 及
`sent`/`original` 双帧对照；采集器应将该序号标为首个异常并自动重建。

观察采集器行为：`watch curl -s localhost:8001/status`（重点看 `verification` 段）。

典型演练（迟到快照）：先设 `snapshot_delay=5`，再 `disconnect` —— 采集器进入 503；
快照在途期间再次 `disconnect`，旧快照响应迟到后被丢弃（`stale_snapshots` +1），
新世代重新取快照衔接后才恢复 200。

## API 一览

**采集器**（8001）
- `GET /book` — 已同步：`{symbol, seq, digest, bids, asks}`；
  失步：`503 {error, detail, anomaly_seq, last_error}`
- `GET /status` — 同步状态、序号、世代、两侧档数、`verification` 核验段、统计计数、配置
- `GET /verification` — 核验专视图：算法、链头、已核验条数、失败次数、
  当前首个异常序号（`anomaly_seq`）、留痕（`last_anomaly`）

`verification` 段字段：

| 字段 | 说明 |
|---|---|
| `algo` | 摘要链算法版本（`sha256-chain-v1`） |
| `chain_ok` | 当前链可信（已同步且无未恢复异常） |
| `chain_head` | 已核验的摘要链头（对应 `seq`，可与行情源 `/status.digest` 比对） |
| `verified_increments` | 累计通过核验的增量条数 |
| `failures` | 累计核验失败次数 |
| `anomaly_seq` / `anomaly_reason` | 当前未恢复 episode 的首个异常序号/原因（恢复后清空） |
| `last_anomaly` | 最近一次异常留痕 `{seq, reason, generation}`（恢复后保留） |

**行情源**（8000）
- `GET /snapshot`、`WS /ws`、`GET /status`（含 `digest` 链头）、`POST /control/*`（见上）

## 目录结构

```
├── docker-compose.yml
├── collector/                 # 采集器
│   ├── app/digest.py          #   摘要链算法（与行情源副本一致）
│   ├── app/orderbook.py       #   订单簿 + 快照衔接 + 链核验（纯逻辑）
│   ├── app/sync_manager.py    #   WS/快照/世代管理/核验/自愈循环
│   ├── app/main.py            #   FastAPI: /book /status /verification
│   └── tests/                 #   pytest 单测（29 项）
├── market_source/
│   ├── app/digest.py          # 摘要链算法（与采集器副本一致）
│   └── app/main.py            # 行情源 + 故障注入控制面（含传输篡改）
└── scripts/smoke_test.py      # 端到端冒烟（丢序/断线/延迟快照/传输篡改/序号单调）
```

## 环境变量（采集器）

| 变量 | 默认 | 说明 |
|---|---|---|
| `SYMBOL` | `CU2601` | 合约 |
| `SOURCE_HTTP` / `SOURCE_WS` | compose 内自动指向行情源 | 行情源地址 |
| `MAX_LEVELS` | `20` | 每侧最大档数（超限判协议错误并重同步） |
| `RECONNECT_DELAY` | `1.0` | 重同步间隔（秒） |
| `DATA_TIMEOUT` | `5.0` | 收不到任何帧（含心跳）判定断流（秒） |
| `SNAPSHOT_TIMEOUT` | `30.0` | 快照 HTTP 超时（需大于注入的快照延迟） |
