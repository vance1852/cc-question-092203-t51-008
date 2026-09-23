# 换电站运营管理平台（纯后端）

新能源物流车换电站后台管理的纯后端 API 服务，提供站点、车辆和换电记录的统一管理能力。

## 技术栈

- FastAPI + Uvicorn
- SQLAlchemy + SQLite（本地文件，开箱即用）
- PyJWT（JWT 鉴权）
- 密码哈希用标准库 `hashlib.pbkdf2_hmac`，无额外依赖

所有数据本地、离线可运行，不依赖任何外部服务。

## 运行

```bash
pip install -r requirements.txt
python run.py
```

服务启动在 `http://127.0.0.1:7634`，首次启动自动建表并灌入种子数据。
交互式文档：`http://127.0.0.1:7634/docs`。

## 内置账号

首次启动自动创建唯一管理员（本平台只有 admin 一个角色）：

- 用户名：`admin`
- 密码：`admin123`

## 已实现的基础功能

- 登录签发 JWT、获取当前用户（`/api/auth/login`、`/api/auth/me`）
- 换电站增删改查（`/api/stations`）
- 车辆增删改查（`/api/vehicles`）
- 换电记录查询与登记（`/api/swaps`，会联动更新车辆电量与站点可用电池）
- 仪表盘统计（`/api/dashboard/stats`）
- 健康检查（`/api/health`）

除 `login` 与 `health` 外，所有接口均需携带 `Authorization: Bearer <token>`。

## 山区终端离线事件批量同步

断网期间终端把换电结果缓存在本地，恢复连接后批量重传。同步协议保证：
**重传不重复扣库存、乱序可排、缺口补齐自动续推、在线交易不被旧离线事件倒写。**

### 终端协议约定

每笔事件携带三个稳定定位字段：

| 字段 | 含义 |
| --- | --- |
| `event_uid` | 稳定事件标识（建议 UUID），事件首次产生时生成，**重传整批保持不变**，服务端据此幂等去重 |
| `generation` | 会话代次，设备重置/重新建档后必须更换（建议 UUID），另带 `generation_started_at` 起始时间 |
| `seq` | 同代次内从 1 开始严格递增的序号，服务端按序号连续应用，不允许跳号 |

### 接收结果四态（全部持久化，服务重启不丢）

- `applied`：顺序与实体状态前提均满足，已与库存扣减、车辆电量更新在**同一事务**原子落入换电链路；
- `duplicate`：`event_uid` 此前已生效，本次为重传副本，**不会再次扣库存**；
- `waiting`：已接收落库，但前序序号存在缺口，挂起等待补齐（补齐后自动续推）；
- `conflict`：业务冲突（无可用电池/实体不存在/换电后电量未升高/同序号事件标识不一致/旧事件倒写），
  阻断顺序推进，等待人工处理。

### 接口

- `POST /api/sync/batches`：批量提交（请求体可原样安全重发），返回逐笔结果与 `confirmed_seq`；
- `GET  /api/sync/devices/{device_id}`：查询确认范围——`confirmed_seq` 以内的本地缓存可安全清理，
  `missing_seqs` 为尚缺序号，`has_open_conflict` 提示是否需要后台介入；
- `GET  /api/sync/events?device_id=&status_filter=`：事件队列（冲突处理台），含原始载荷 JSON 与 SHA-256 摘要；
- `POST /api/sync/events/{id}/resolve`：人工解决，`action=apply|drop`，**必须填写 reason**，
  处理人、时间、原因与原始载荷摘要一并留痕；定论后闸门自动继续后续事件；
- `POST /api/sync/pump`：手动触发全设备续推（服务启动时也会自动执行一次）；
- `GET  /api/sync/reconcile`：对账结果，证明每个生效事件恰好对应一条换电记录、无重复应用、无旧事件倒写。

### 关键保障

- `swap_records.source_event_id` 唯一约束 + 同事务扣减：数据库层保证一个离线事件至多扣一次库存；
- 车辆 `last_swapped_at` 时间闸：晚到的、发生时间更早的离线事件（或旧会话代次重放）会被判冲突拒绝，
  在线交易结果不会被旧离线事件覆盖；
- 每个批次事务以设备游标写语句开始（SQLite WAL + busy_timeout 串行化），同设备并发提交安全。

## 测试

```bash
pip install -r requirements.txt
pytest -q
```

## 编码说明

源码与数据均为 UTF-8；FastAPI 响应为 UTF-8 JSON，中文不转义、不乱码。
Windows 控制台若为 GBK，仅影响终端打印观感，不影响接口返回。
