"""离线事件批量同步领域服务。

设计要点：
- 每个终端一行 ``DeviceCursor``（会话代次 + 连续确认序号），每个事件一行 ``SyncEvent``。
- 事件按 (会话代次, 序号) 严格队首推进：前序缺号则后续事件挂起 waiting；
  缺口补齐（真实事件到达或人工 skip-gap）后自动继续 drain。
- 只有顺序前提与实体/业务前提都满足的事件，才在同一事务内原子落入现有
  换电链路（写 SwapRecord、扣站点电池、按时间戳决定是否回写车辆电量）。
- ``swap_records.sync_event_id`` 唯一约束 + 事件终态机在数据库层保证
  「每个接受事件至多影响库存一次」。
- 旧业务时间的事件（含断网期间积压、晚到的离线事件）只记账不回写车辆电量，
  在线交易的较新电量不会被倒写。
"""
from __future__ import annotations

import hashlib
import json
import threading
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy.orm import Session

from ..models import (
    DeviceCursor,
    EVENT_FINAL_STATUSES,
    EVENT_TRANSIENT_STATUSES,
    Station,
    SwapRecord,
    SyncEvent,
    SyncRejectedDelivery,
    Vehicle,
)

# 进程内按设备串行化写入（单进程部署足够；配合 SQLite 单写者语义）
_device_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)


def device_lock(device_id: str) -> threading.Lock:
    return _device_locks[device_id]


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------

def canonical_payload(data: dict[str, Any]) -> str:
    """规范化 JSON：字段排序、紧凑分隔，使重传载荷摘要稳定可比。"""
    return json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)


def payload_hash_of(canonical: str) -> str:
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def coerce_utc_naive(value: Any) -> datetime:
    """把输入时间统一为 naive UTC（库内全部按 UTC 存储与比较）。"""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str) and value:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        return datetime.utcnow()
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _gap_event_id(device_id: str, generation: int, seq: int) -> str:
    return f"__skipped_gap__:{device_id}:{generation}:{seq}"


def _rejected_delivery(
    db: Session,
    device_id: str,
    generation: int,
    seq: int,
    event_id: str,
    canonical: str,
    reason: str,
    now: datetime,
) -> dict[str, Any]:
    """协议级拒收持久化（按 event_id 幂等 upsert），返回结果 dict。"""
    row = db.query(SyncRejectedDelivery).filter(
        SyncRejectedDelivery.event_id == event_id
    ).first()
    if row is None:
        row = SyncRejectedDelivery(
            device_id=device_id,
            session_generation=generation,
            seq=seq,
            event_id=event_id,
            raw_payload=canonical,
            payload_hash=payload_hash_of(canonical),
            reason=reason,
            first_seen=now,
            last_seen=now,
        )
        db.add(row)
    else:
        row.last_seen = now
        reason = row.reason  # 重传沿用首次拒收原因
    return {
        "event_id": event_id,
        "seq": seq,
        "status": "rejected",
        "detail": reason,
        "swap_record_id": None,
        "payload_hash": payload_hash_of(canonical),
    }


def _get_or_create_cursor(db: Session, device_id: str, generation: int) -> DeviceCursor:
    cursor = db.get(DeviceCursor, device_id)
    if cursor is None:
        cursor = DeviceCursor(
            device_id=device_id,
            session_generation=generation,
            applied_seq=0,
            max_seen_seq=0,
        )
        db.add(cursor)
        db.flush()
    return cursor


def _events_by_seq(db: Session, device_id: str, generation: int) -> dict[int, SyncEvent]:
    rows = (
        db.query(SyncEvent)
        .filter(
            SyncEvent.device_id == device_id,
            SyncEvent.session_generation == generation,
        )
        .all()
    )
    return {row.seq: row for row in rows}


# ---------------------------------------------------------------------------
# 事件应用：原子落入现有换电链路
# ---------------------------------------------------------------------------

def _resolve_vehicle(db: Session, data: dict[str, Any]) -> tuple[Optional[Vehicle], Optional[str]]:
    """按载荷定位车辆。给了 vehicle_id 以其为准，否则用车牌。"""
    vehicle_id = data.get("vehicle_id")
    if vehicle_id is not None:
        vehicle = db.get(Vehicle, vehicle_id)
        if vehicle is None:
            return None, f"车辆不存在：vehicle_id={vehicle_id}"
        return vehicle, None
    plate = (data.get("vehicle_plate") or "").strip()
    vehicle = db.query(Vehicle).filter(Vehicle.plate == plate).first()
    if vehicle is None:
        return None, f"车辆不存在：车牌 {plate}"
    return vehicle, None


def _apply_event(db: Session, ev: SyncEvent) -> tuple[str, Optional[str]]:
    """尝试把队首事件落入换电链路。

    返回 (结果状态, 说明)：
      applied  —— 已原子落账
      conflict —— 业务前提冲突，需人工裁决
      rejected —— 载荷/实体永久无效
    所有写入均在调用方事务内，异常时整体回滚。
    """
    try:
        data = json.loads(ev.raw_payload or "{}")
    except (ValueError, TypeError):
        return "rejected", "原始载荷不是合法 JSON"

    station = db.get(Station, data.get("station_id"))
    if station is None:
        return "rejected", f"换电站不存在：station_id={data.get('station_id')}"
    vehicle, problem = _resolve_vehicle(db, data)
    if vehicle is None:
        return "rejected", problem

    soc_before = float(data.get("soc_before", 0.0))
    soc_after = float(data.get("soc_after", 0.0))

    # 业务前提（可人工裁决后强制应用）
    if station.battery_ready <= 0:
        return "conflict", f"站点 {station.name} 暂无满电电池可换（当前 0 块）"
    if soc_after <= soc_before:
        return "conflict", f"换电后电量 {soc_after} 不高于换电前 {soc_before}"

    # 原子落账：SwapRecord 唯一 sync_event_id 由数据库兜底防重
    record = SwapRecord(
        vehicle_id=vehicle.id,
        station_id=station.id,
        soc_before=soc_before,
        soc_after=soc_after,
        swapped_at=ev.occurred_at,
        sync_event_id=ev.event_id,
    )
    db.add(record)
    station.battery_ready -= 1

    # 旧业务时间事件不得倒写较新的车辆电量（在线交易或更晚的离线事件）
    suppressed = (
        vehicle.soc_updated_at is not None
        and vehicle.soc_updated_at > ev.occurred_at
    )
    if suppressed:
        ev.soc_write_suppressed = True
        ev.detail = (
            f"已扣库存并记账；事件时间 {ev.occurred_at:%Y-%m-%d %H:%M:%S} 早于车辆当前电量的"
            f"更新时间 {vehicle.soc_updated_at:%Y-%m-%d %H:%M:%S}，电量回写已抑制"
        )
    else:
        vehicle.current_soc = soc_after
        vehicle.soc_updated_at = ev.occurred_at

    ev.status = "applied"
    ev.applied_at = datetime.utcnow()
    ev.swap_record = record
    db.flush()  # 取得 record.id 并触发唯一约束检查
    if suppressed:
        return "applied", ev.detail
    return "applied", None


def _force_apply_event(db: Session, ev: SyncEvent) -> tuple[str, Optional[str]]:
    """人工强制应用：跳过电量单调性等软前提，但库存物理不足仍无法执行。"""
    try:
        data = json.loads(ev.raw_payload or "{}")
    except (ValueError, TypeError):
        return "rejected", "原始载荷不是合法 JSON"
    station = db.get(Station, data.get("station_id"))
    if station is None:
        return "rejected", f"换电站不存在：station_id={data.get('station_id')}"
    vehicle, problem = _resolve_vehicle(db, data)
    if vehicle is None:
        return "rejected", problem
    if station.battery_ready <= 0:
        return "conflict", "库存仍为 0 块满电电池，无法强制扣减，请先补充库存后再执行"

    soc_before = float(data.get("soc_before", 0.0))
    soc_after = float(data.get("soc_after", 0.0))
    record = SwapRecord(
        vehicle_id=vehicle.id,
        station_id=station.id,
        soc_before=soc_before,
        soc_after=soc_after,
        swapped_at=ev.occurred_at,
        sync_event_id=ev.event_id,
    )
    db.add(record)
    station.battery_ready -= 1
    suppressed = (
        vehicle.soc_updated_at is not None
        and vehicle.soc_updated_at > ev.occurred_at
    )
    if suppressed:
        ev.soc_write_suppressed = True
    else:
        vehicle.current_soc = soc_after
        vehicle.soc_updated_at = ev.occurred_at
    ev.status = "applied"
    ev.applied_at = datetime.utcnow()
    ev.swap_record = record
    db.flush()
    return "applied", "人工裁决强制应用" + ("；旧事件电量回写已抑制" if suppressed else "")


# ---------------------------------------------------------------------------
# 队首推进
# ---------------------------------------------------------------------------

def _drain(db: Session, cursor: DeviceCursor) -> None:
    """从连续确认序号之后逐条推进，直到缺号或业务冲突为止。

    推进只依赖持久化状态，因此服务重启后下一次接收（或人工操作）会自动
    继续处理可推进的记录，待处理队列不会丢失。
    """
    by_seq = _events_by_seq(db, cursor.device_id, cursor.session_generation)

    frontier = cursor.applied_seq + 1
    blocked: Optional[str] = None
    blocked_seq: Optional[int] = None
    while True:
        ev = by_seq.get(frontier)
        if ev is None:
            blocked, blocked_seq = "gap", frontier
            break
        if ev.status in ("applied", "rejected", "skipped_gap", "duplicate"):
            # 已终结（或不占序号的重传副本）：向前推进
            frontier += 1
            continue
        if ev.status == "conflict":
            blocked, blocked_seq = "conflict", frontier
            break

        # new / waiting：前序已齐，尝试原子落账
        result, detail = _apply_event(db, ev)
        ev.status = result
        ev.detail = detail
        if result == "conflict":
            blocked, blocked_seq = "conflict", frontier
            break
        if result == "rejected":
            ev.resolution_reason = ev.resolution_reason or detail
            frontier += 1
            continue
        frontier += 1  # applied

    cursor.applied_seq = frontier - 1

    # 后续已到达但尚不能处理的事件持久化为 waiting（"等待前序"）
    has_pending = any(seq > cursor.applied_seq for seq in by_seq)
    if blocked == "gap":
        waiting_detail = f"等待前序序号 {blocked_seq} 到达" if has_pending else None
    elif blocked == "conflict":
        waiting_detail = f"等待序号 {blocked_seq} 的业务冲突人工裁决"
    else:
        waiting_detail = None
    for seq, later in by_seq.items():
        if seq <= cursor.applied_seq:
            continue
        if later.status in ("new", "waiting"):
            later.status = "waiting"
            later.detail = waiting_detail

    # 只有确有后续事件被缺口/冲突挡住才记录阻塞；空队列（只是尚未见到下一序号）不算积压
    really_blocked = blocked == "conflict" or (blocked == "gap" and has_pending)
    if really_blocked:
        if cursor.blocked_since is None:
            cursor.blocked_since = datetime.utcnow()
    else:
        cursor.blocked_since = None
    cursor.updated_at = datetime.utcnow()


# ---------------------------------------------------------------------------
# 批量接收入口
# ---------------------------------------------------------------------------

def ingest_batch(
    db: Session,
    device_id: str,
    generation: int,
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    """接收一批终端事件（可安全重发），落库后推进队列。

    items 为已通过 Pydantic 校验的事件 dict（含 event_id/seq/...）。
    """
    with _device_locks[device_id]:
        cursor = _get_or_create_cursor(db, device_id, generation)

        # 1) 会话代次处理
        if generation < cursor.session_generation:
            # 设备重置前旧代次的迟到/重传批次：不进入当前队列
            results = []
            now = datetime.utcnow()
            for item in items:
                canonical = canonical_payload(item)
                existing = (
                    db.query(SyncEvent).filter(SyncEvent.event_id == item["event_id"]).first()
                )
                if existing is not None:
                    # 该事件此前已接收（已应用/已终结）：重传只回 duplicate
                    results.append(
                        _result_for(existing, duplicate_note=True, canonical=canonical)
                    )
                else:
                    # 旧代次中从未见过的事件：持久化拒收留痕（幂等）
                    results.append(
                        _rejected_delivery(
                            db,
                            device_id,
                            generation,
                            item["seq"],
                            item["event_id"],
                            canonical,
                            f"旧会话代次 {generation}（当前代次 "
                            f"{cursor.session_generation}），设备重置前事件不再入队",
                            now,
                        )
                    )
            db.commit()
            return _batch_response(cursor, results)

        if generation > cursor.session_generation:
            # 设备重置、代次轮转：旧代次中仍挂起的事件终结，游标新代次从 1 开始
            stale = (
                db.query(SyncEvent)
                .filter(
                    SyncEvent.device_id == device_id,
                    SyncEvent.session_generation < generation,
                    SyncEvent.status.in_(list(EVENT_TRANSIENT_STATUSES)),
                )
                .all()
            )
            now = datetime.utcnow()
            for ev in stale:
                ev.status = "rejected"
                ev.detail = f"设备已进入会话代次 {generation}，旧代次 {ev.session_generation} 挂起事件终结"
                ev.resolution_reason = "会话代次轮转"
                ev.resolved_at = now
            cursor.session_generation = generation
            cursor.applied_seq = 0
            cursor.blocked_since = None

        # 2) 事件 upsert（按稳定坐标与稳定事件标识去重）
        touched: list[SyncEvent] = []
        results: list[dict[str, Any]] = []
        now = datetime.utcnow()
        for item in items:
            canonical = canonical_payload(item)
            digest = payload_hash_of(canonical)
            occurred_at = coerce_utc_naive(item.get("occurred_at"))

            ev = (
                db.query(SyncEvent)
                .filter(
                    SyncEvent.device_id == device_id,
                    SyncEvent.session_generation == generation,
                    SyncEvent.seq == item["seq"],
                )
                .first()
            )
            by_event_id = (
                db.query(SyncEvent).filter(SyncEvent.event_id == item["event_id"]).first()
            )

            if ev is not None and ev.event_id != item["event_id"]:
                results.append(
                    _rejected_delivery(
                        db,
                        device_id,
                        generation,
                        item["seq"],
                        item["event_id"],
                        canonical,
                        f"序号 {item['seq']} 已被事件 {ev.event_id} 占用，event_id 必须稳定",
                        now,
                    )
                )
                continue
            if by_event_id is not None and ev is None:
                results.append(
                    _rejected_delivery(
                        db,
                        device_id,
                        generation,
                        item["seq"],
                        item["event_id"],
                        canonical,
                        f"event_id 曾用于代次 {by_event_id.session_generation} 序号 "
                        f"{by_event_id.seq}，不得复用或改变坐标",
                        now,
                    )
                )
                continue

            if ev is None:
                ev = SyncEvent(
                    device_id=device_id,
                    session_generation=generation,
                    seq=item["seq"],
                    event_id=item["event_id"],
                    event_type=item.get("event_type", "swap_completed"),
                    raw_payload=canonical,
                    payload_hash=digest,
                    occurred_at=occurred_at,
                    status="new",
                )
                db.add(ev)
                db.flush()
                touched.append(ev)
                cursor.max_seen_seq = max(cursor.max_seen_seq, ev.seq)
            else:
                # 重传：刷新 last_seen；载荷变化只留痕，绝不重新应用
                ev.last_seen = now
                note = None
                if ev.payload_hash != digest:
                    note = "重传载荷与首次接收不一致，已按首次载荷处理并留痕"
                results.append(_result_for(ev, duplicate_note=True, canonical=canonical, extra_note=note))
                continue

        # 3) 严格按序推进
        _drain(db, cursor)
        db.commit()

        # 4) 组装本批新事件结果（重传结果已在上面追加）
        for ev in touched:
            results.append(_result_for(ev))
        results.sort(key=lambda r: r["seq"])
        return _batch_response(cursor, results)


def _result_for(
    ev: SyncEvent,
    duplicate_note: bool = False,
    canonical: Optional[str] = None,
    extra_note: Optional[str] = None,
) -> dict[str, Any]:
    if duplicate_note:
        # 这是一次重复送达：统一回报 duplicate，并在 detail 中给出服务端权威状态
        status = "duplicate"
        detail = f"重复送达，服务端持久状态：{ev.status}"
        if ev.detail:
            detail += f"（{ev.detail}）"
    else:
        status = ev.status
        detail = ev.detail
    if extra_note:
        detail = f"{extra_note}；{detail}" if detail else extra_note
    return {
        "event_id": ev.event_id,
        "seq": ev.seq,
        "status": status,
        "detail": detail,
        "swap_record_id": ev.swap_record_id,
        "payload_hash": ev.payload_hash,
    }


def _batch_response(cursor: DeviceCursor, results: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "device_id": cursor.device_id,
        "session_generation": cursor.session_generation,
        "confirmed_seq": cursor.applied_seq,
        "max_seen_seq": cursor.max_seen_seq,
        "blocked_since": cursor.blocked_since,
        "results": results,
    }


# ---------------------------------------------------------------------------
# 人工裁决
# ---------------------------------------------------------------------------

def resolve_conflict(
    db: Session,
    device_id: str,
    event_id: str,
    action: str,
    reason: str,
    username: str,
) -> SyncEvent:
    """对 conflict 事件人工裁决（force_apply / reject），必须留原因，随后自动续推。"""
    with _device_locks[device_id]:
        cursor = db.get(DeviceCursor, device_id)
        if cursor is None:
            raise LookupError("设备不存在")
        ev = (
            db.query(SyncEvent)
            .filter(SyncEvent.device_id == device_id, SyncEvent.event_id == event_id)
            .first()
        )
        if ev is None:
            raise LookupError("事件不存在")
        if ev.session_generation != cursor.session_generation:
            raise ValueError("事件属于旧会话代次，不能在当前代次裁决")
        if ev.status != "conflict":
            raise ValueError(f"仅 conflict 事件可裁决，当前状态 {ev.status}")

        now = datetime.utcnow()
        ev.resolution_reason = reason
        ev.resolved_by = username
        ev.resolved_at = now
        if action == "reject":
            ev.status = "rejected"
            ev.detail = f"人工驳回：{reason}"
        else:
            result, detail = _force_apply_event(db, ev)
            if result != "applied":
                db.rollback()
                raise ConflictStillBusy(detail or "仍无法应用")
            ev.resolution_reason = f"强制应用：{reason}"
            ev.detail = detail

        _drain(db, cursor)
        db.commit()
        db.refresh(ev)
        return ev


class ConflictStillBusy(Exception):
    """强制应用时业务前提仍不满足（如库存为 0）。"""


def skip_gap(
    db: Session,
    device_id: str,
    seq: int,
    reason: str,
    username: str,
    generation: Optional[int] = None,
) -> SyncEvent:
    """人工确认某个缺号永久丢失，写入 skipped_gap 墓碑并自动续推。"""
    with _device_locks[device_id]:
        cursor = db.get(DeviceCursor, device_id)
        if cursor is None:
            raise LookupError("设备不存在")
        gen = generation or cursor.session_generation
        if gen != cursor.session_generation:
            raise ValueError("只能为当前会话代次登记缺号跳过")
        existing = (
            db.query(SyncEvent)
            .filter(
                SyncEvent.device_id == device_id,
                SyncEvent.session_generation == gen,
                SyncEvent.seq == seq,
            )
            .first()
        )
        if existing is not None:
            raise ValueError(f"序号 {seq} 已存在真实事件（{existing.event_id}），不能跳过")
        if seq <= cursor.applied_seq:
            raise ValueError(f"序号 {seq} 已在确认范围内，无需跳过")

        marker = canonical_payload(
            {"device_id": device_id, "generation": gen, "seq": seq, "kind": "skipped_gap"}
        )
        tombstone = SyncEvent(
            device_id=device_id,
            session_generation=gen,
            seq=seq,
            event_id=_gap_event_id(device_id, gen, seq),
            event_type="__skipped_gap__",
            raw_payload=marker,
            payload_hash=payload_hash_of(marker),
            occurred_at=datetime.utcnow(),
            status="skipped_gap",
            detail=f"人工确认缺号跳过：{reason}",
            resolution_reason=reason,
            resolved_by=username,
            resolved_at=datetime.utcnow(),
            applied_at=datetime.utcnow(),
        )
        db.add(tombstone)
        cursor.max_seen_seq = max(cursor.max_seen_seq, seq)
        db.flush()
        _drain(db, cursor)
        db.commit()
        db.refresh(tombstone)
        return tombstone


# ---------------------------------------------------------------------------
# 查询与对账
# ---------------------------------------------------------------------------

def recover_pending(db: Session) -> dict[str, int]:
    """服务启动时恢复：基于持久化状态重新推进所有设备队列。

    队列状态全部落库，重启不丢；此处逐设备重放 drain，使重启前因进程崩溃
    未续推的可推进记录（例如缺口已在最后一批补齐）自动继续处理。
    返回各状态变化计数（恢复的设备数）。
    """
    recovered = 0
    for cursor in db.query(DeviceCursor).order_by(DeviceCursor.device_id).all():
        with _device_locks[cursor.device_id]:
            before = cursor.applied_seq
            _drain(db, cursor)
            if cursor.applied_seq != before:
                recovered += 1
    db.commit()
    return {"devices_recovered": recovered}


def get_cursor(db: Session, device_id: str) -> DeviceCursor:
    cursor = db.get(DeviceCursor, device_id)
    if cursor is None:
        raise LookupError("设备不存在")
    return cursor


def list_events(
    db: Session,
    device_id: str,
    generation: Optional[int] = None,
    status_filter: Optional[str] = None,
) -> list[SyncEvent]:
    query = db.query(SyncEvent).filter(SyncEvent.device_id == device_id)
    if generation is not None:
        query = query.filter(SyncEvent.session_generation == generation)
    if status_filter:
        query = query.filter(SyncEvent.status == status_filter)
    return query.order_by(SyncEvent.session_generation, SyncEvent.seq).all()


def reconcile(db: Session, device_id: Optional[str] = None) -> dict[str, Any]:
    """对账：证明库存至多一次影响、在线电量不被旧离线事件倒写。"""
    event_query = db.query(SyncEvent)
    if device_id:
        event_query = event_query.filter(SyncEvent.device_id == device_id)
    events = event_query.all()

    status_counts: dict[str, int] = defaultdict(int)
    for ev in events:
        status_counts[ev.status] += 1

    applied = [e for e in events if e.status == "applied"]
    applied_ids = [e.event_id for e in applied]

    # 1) 每个 applied 事件必须有关联换电记录
    swap_rows = (
        db.query(SwapRecord)
        .filter(SwapRecord.sync_event_id.isnot(None))
    )
    if device_id:
        # 离线换电记录通过事件反查设备
        device_event_ids = [
            e.event_id for e in events if e.status in EVENT_FINAL_STATUSES
        ]
        swap_rows = swap_rows.filter(SwapRecord.sync_event_id.in_(device_event_ids or []))
    swap_rows = swap_rows.all()
    offline_swap_row_count = len(swap_rows)

    applied_without_swap = [e for e in applied if not e.swap_record_id]
    swap_ids = [r.sync_event_id for r in swap_rows]
    swap_without_applied = [sid for sid in swap_ids if sid not in applied_ids]
    # 同一 event_id 出现多条换电记录（数据库唯一约束本应杜绝，对账再次核实）
    duplicate_inventory = len(swap_ids) != len(set(swap_ids))
    every_applied_has_swap = not applied_without_swap and not swap_without_applied
    inventory_at_most_once = (
        every_applied_has_swap
        and not duplicate_inventory
        and offline_swap_row_count == len(applied)
    )

    suppressed_count = sum(1 for e in applied if e.soc_write_suppressed)

    # 2) 在线交易不被旧离线事件倒写：
    #    车辆当前电量必须等于「实际写入过车辆的、业务时间最新的一笔」结果。
    #    在线记录（sync_event_id IS NULL）与未抑制的离线应用事件都是有效写入；
    #    被抑制的旧离线事件虽记账（有 SwapRecord），但不参与车辆状态时间线。
    vehicles = db.query(Vehicle).filter(Vehicle.soc_updated_at.isnot(None)).all()
    applied_by_id = {e.event_id: e for e in applied}
    overwrite_violations: list[str] = []
    for vehicle in vehicles:
        winner_time: Optional[datetime] = None
        winner_id: int = -1
        winner_soc: Optional[float] = None
        for record in vehicle.swaps:
            if record.sync_event_id is not None:
                ev = applied_by_id.get(record.sync_event_id)
                # 被抑制的离线写入不得成为车辆当前电量来源
                if ev is None or ev.soc_write_suppressed:
                    continue
            # 同业务时间按记录 id（即应用先后）决胜：序号靠后的后写者胜
            if winner_time is None or (record.swapped_at, record.id) > (winner_time, winner_id):
                winner_time, winner_id, winner_soc = record.swapped_at, record.id, record.soc_after
        if winner_soc is not None and (
            vehicle.current_soc != winner_soc or vehicle.soc_updated_at != winner_time
        ):
            overwrite_violations.append(
                f"车辆 {vehicle.plate} 当前电量 {vehicle.current_soc}（时间线 "
                f"{vehicle.soc_updated_at:%Y-%m-%d %H:%M:%S}）与最新有效写入结果 "
                f"{winner_soc}（{winner_time:%Y-%m-%d %H:%M:%S}）不一致，疑似被旧事件倒写"
            )

    # 3) 游标与事件连续性核对
    cursor_query = db.query(DeviceCursor)
    if device_id:
        cursor_query = cursor_query.filter(DeviceCursor.device_id == device_id)
    cursors = cursor_query.all()
    device_reports = []
    for cursor in cursors:
        counts: dict[str, int] = defaultdict(int)
        for ev in events:
            if ev.device_id == cursor.device_id and ev.session_generation == cursor.session_generation:
                counts[ev.status] += 1
        device_reports.append(
            {
                "device_id": cursor.device_id,
                "session_generation": cursor.session_generation,
                "applied_seq": cursor.applied_seq,
                "max_seen_seq": cursor.max_seen_seq,
                "blocked_since": cursor.blocked_since,
                "counts": dict(counts),
            }
        )

    return {
        "scope_device_id": device_id,
        "status_counts": dict(status_counts),
        "applied_count": len(applied),
        "offline_swap_row_count": offline_swap_row_count,
        "soc_write_suppressed_count": suppressed_count,
        "every_applied_has_swap": every_applied_has_swap,
        "inventory_effect_at_most_once": inventory_at_most_once,
        "online_not_overwritten_by_stale": not overwrite_violations,
        "overwrite_violations": overwrite_violations,
        "devices": device_reports,
        "events": [
            {
                "id": e.id,
                "device_id": e.device_id,
                "session_generation": e.session_generation,
                "seq": e.seq,
                "event_id": e.event_id,
                "event_type": e.event_type,
                "raw_payload": e.raw_payload,
                "payload_hash": e.payload_hash,
                "occurred_at": e.occurred_at,
                "status": e.status,
                "detail": e.detail,
                "swap_record_id": e.swap_record_id,
                "soc_write_suppressed": e.soc_write_suppressed,
                "resolution_reason": e.resolution_reason,
                "resolved_by": e.resolved_by,
                "resolved_at": e.resolved_at,
                "first_seen": e.first_seen,
                "last_seen": e.last_seen,
                "applied_at": e.applied_at,
            }
            for e in sorted(events, key=lambda x: (x.device_id, x.session_generation, x.seq))
        ],
    }
