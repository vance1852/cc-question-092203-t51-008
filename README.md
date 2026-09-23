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

## 山区站点离线事件批量同步

终端断网期间把换电结果缓存在本地，恢复连接后批量补传。服务端保证重传幂等、
乱序可纠、缺号可补、冲突可裁决，并且每个接受事件至多影响库存一次、
在线交易电量不被旧离线事件倒写。

### 终端协议三要素

- `event_id`：终端生成的**稳定事件标识**，同一次换电的重传必须保持不变
- `session_generation`：**会话代次**，设备重置/重新开始序号时单调递增
- `seq`：代次内从 1 开始的**严格递增序号**

### 事件状态（逐事件持久化）

| 状态 | 含义 |
| --- | --- |
| `applied` | 已原子落入换电链路（写换电记录、扣站点电池） |
| `duplicate` | 重复送达，返回服务端权威状态，无副作用 |
| `waiting` | 前序序号未齐，等待补齐（补齐后自动续推） |
| `conflict` | 业务前提冲突（如无可用电池），等待人工裁决 |
| `rejected` | 永久无效（实体不存在、旧代次、坐标冲突等） |
| `skipped_gap` | 人工确认缺号永久丢失的墓碑 |

### 接口

- `POST /api/sync/batch`：批量同步入口，**同一批可安全任意重发**
- `GET /api/sync/devices/{device_id}/cursor`：查询设备游标
  （当前代次、连续确认序号 `applied_seq`、积压起点），终端据此清理本地缓存
- `GET /api/sync/devices/{device_id}/events`：逐事件接收结果（可按代次/状态过滤）
- `POST /api/sync/devices/{device_id}/events/{event_id}/resolve`：
  人工裁决冲突（`force_apply`/`reject`），**必须填写原因**，原始载荷 sha256 摘要随事件留存
- `POST /api/sync/devices/{device_id}/skip-gap`：登记缺号墓碑并自动续推
- `GET /api/sync/reconcile`：对账报告

批量响应中的 `confirmed_seq` 表示 `[1, confirmed_seq]` 已全部终结，
该范围内的本地缓存可安全删除。

### 关键保证

1. **库存至多一次**：`swap_records.sync_event_id` 数据库唯一约束兜底，
   事件落账与库存扣减在同一事务，任何重传/并发都不会二次扣库存。
2. **严格按序**：前序缺号则后续一律 `waiting`，绝不让后发的换电先扣库存；
   缺口由真实事件或人工墓碑补齐后自动 drain。
3. **旧事件不倒写**：每笔换电按终端业务时间 `occurred_at` 落账；晚到的旧事件
   仍记账扣库存，但当车辆已有更新时间的电量（在线交易或更晚离线事件）时
   **抑制电量回写**并在事件 `detail` 留痕。
4. **重启不丢**：队列状态全部持久化，服务启动时自动重放推进，
   待处理记录不因重启丢失。
5. **代次轮转**：设备重置后新代次从 1 开始，旧代次挂起事件终结留痕，
   旧代次迟到事件拒收，不会污染新序号空间。
6. **对账可证**：`/api/sync/reconcile` 交叉核对事件与换电记录一一对应
   （`inventory_effect_at_most_once`）及车辆电量未被旧事件倒写
   （`online_not_overwritten_by_stale`）。

## 测试

```bash
pip install -r requirements.txt
pytest -q
```

## 编码说明

源码与数据均为 UTF-8；FastAPI 响应为 UTF-8 JSON，中文不转义、不乱码。
Windows 控制台若为 GBK，仅影响终端打印观感，不影响接口返回。
