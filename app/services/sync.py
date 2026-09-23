"""离线换电事件批量同步核心。

职责：
1. 幂等接收：稳定事件标识 (device_id, event_uid) 去重，整批可安全重发；
2. 顺序闸门：按 (generation, seq) 连续推进，缺序号挂起，补齐后自动续推；
3. 四态持久化：applied / duplicate（仅响应态）/ waiting / conflict，全部落库，重启不丢；
4. 原子应用：事件落入换电链路与库存扣减、车辆电量更新在同一事务内完成；
5. 防倒写：车辆最近换电时间闸 + SwapRecord.source_event_id 唯一约束，
   保证每个被接受的离线事件至多影响库存一次，在线交易不会被旧离线事件覆盖。

并发说明：SQLite 下每个批次事务以一条设备游标的写语句开始（立即获取 RESERVED
锁，配合 busy_timeout），随后再读游标与事件，保证同设备并发提交严格串行、
后提交者看到先提交者的全部结果，不会出现两个事务同时首传同一事件。
"""
from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..models import DeviceCursor, OfflineEvent, Station, SwapRecord, Vehicle
from ..schemas import OfflineBatchIn, OfflineEventIn, OfflineEventResult

# 持久化状态
ST_WAITING = "waiting"
ST_APPLIED = "applied"
ST_CONFLICT = "conflict"
ST_RESOLVED = "resolved"


class StaleGenerationError(Exception):
    """终端携带的会话代次早于服务端当前代次（旧代次重放）。"""

    def __init__(self, current_generation: str, current_started_at: datetime):
        self.current_generation = current_generation
        self.current_started_at = current_started_at
        super().__init__(f"会话代次已过期，当前代次 {current_generation}")


def as_naive_utc(dt: datetime) -> datetime:
    """统一为朴素 UTC 时间存储（SQLite 列无时区）。"""
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def canonical_payload(event: OfflineEventIn) -> str:
    """业务载荷规范化 JSON（字段排序、紧凑分隔），用于摘要与一致性比对。"""
    body = {
        "vehicle_id": event.vehicle_id,
        "station_id": event.station_id,
        "soc_before": event.soc_before,
        "soc_after": event.soc_after,
        "occurred_at": as_naive_utc(event.occurred_at).isoformat(),
    }
    return json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def payload_sha(canonical: str) -> str:
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _get_cursor(db: Session, device_id: str) -> Optional[DeviceCursor]:
    return db.get(DeviceCursor, device_id)


def _acquire_write_lock(db: Session, device_id: str) -> None:
    """事务首条语句：设备游标写操作，立即取得 SQLite RESERVED 锁。

    即使设备尚不存在（匹配 0 行），写事务也已开始，后续并发提交会在
    busy_timeout 内等待，从而串行化。
    """
    db.execute(
        text(
            "UPDATE device_cursors SET updated_at = CURRENT_TIMESTAMP "
            "WHERE device_id = :d"
        ),
        {"d": device_id},
    )


def _ensure_cursor(db: Session, batch: OfflineBatchIn) -> DeviceCursor:
    """在已持有写锁后读取/注册设备游标，并处理会话代次切换。"""
    started_at = as_naive_utc(batch.generation_started_at)
    cursor = _get_cursor(db, batch.device_id)

    if cursor is None:
        if batch.station_id is not None and db.get(Station, batch.station_id) is None:
            raise ValueError("绑定的换电站不存在")
        cursor = DeviceCursor(
            device_id=batch.device_id,
            station_id=batch.station_id,
            generation=batch.generation,
            generation_started_at=started_at,
            confirmed_seq=0,
            last_seen_seq=0,
        )
        db.add(cursor)
        db.flush()
        return cursor

    if cursor.generation == batch.generation:
        return cursor

    # 代次不同：用代次起始时间裁定新旧（起始时间不更晚一律视为旧代次重放）
    if started_at <= cursor.generation_started_at:
        raise StaleGenerationError(cursor.generation, cursor.generation_started_at)

    # 新代次：旧代次中仍未决的挂起事件不能在新代次下自动应用，转冲突留人工确认；
    # 已应用事件保持不动（其库存影响只发生一次）。
    open_rows = (
        db.query(OfflineEvent)
        .filter(
            OfflineEvent.device_id == batch.device_id,
            OfflineEvent.generation == cursor.generation,
            OfflineEvent.status == ST_WAITING,
        )
        .all()
    )
    for row in open_rows:
        row.status = ST_CONFLICT
        row.conflict_reason = (
            f"设备已切换到新会话代次 {batch.generation}，旧代次 {cursor.generation} "
            "的未决事件需人工确认后处理"
        )

    if batch.station_id is not None:
        if db.get(Station, batch.station_id) is None:
            raise ValueError("绑定的换电站不存在")
        cursor.station_id = batch.station_id
    cursor.generation = batch.generation
    cursor.generation_started_at = started_at
    # 新代次序号从 1 重新计数，确认范围与“见过的最大序号”一并重置
    cursor.confirmed_seq = 0
    cursor.last_seen_seq = 0
    db.flush()
    return cursor


def _ingest_one(
    db: Session, cursor: DeviceCursor, event: OfflineEventIn
) -> tuple[OfflineEvent, bool, Optional[str]]:
    """接收单笔事件并持久化。

    返回 (落库行, 是否本次新建, 重传冲突说明)。
    重传冲突说明非空时，表示同一事件标识携带了与首传不一致的序号/载荷，
    首传行保持定论，本次副本不落第二行。
    """
    canonical = canonical_payload(event)
    digest = payload_sha(canonical)

    # 1) 稳定事件标识去重（跨批次重传、跨乱序报文）
    existing_uid = (
        db.query(OfflineEvent)
        .filter(
            OfflineEvent.device_id == cursor.device_id,
            OfflineEvent.event_uid == event.event_uid,
        )
        .first()
    )
    if existing_uid is not None:
        note: Optional[str] = None
        if (
            existing_uid.generation != cursor.generation
            or existing_uid.seq != event.seq
            or existing_uid.payload_sha256 != digest
        ):
            # 同一事件标识却携带不同序号/载荷：终端协议异常，绝不能按新事件再生效
            note = "同一稳定事件标识对应的序号或载荷与首传不一致，已拒绝重复定义"
        return existing_uid, False, note

    # 2) 同代次同序号是否已被别的事件标识占用（设备重置重发序号 / 重复造号）
    sibling = (
        db.query(OfflineEvent)
        .filter(
            OfflineEvent.device_id == cursor.device_id,
            OfflineEvent.generation == cursor.generation,
            OfflineEvent.seq == event.seq,
        )
        .order_by(OfflineEvent.id)
        .first()
    )
    if sibling is not None:
        reason = (
            f"序号 {event.seq} 已由事件 {sibling.event_uid} 首传，事件标识不一致；"
            + (
                "载荷也不一致，疑似设备重置后序号重用"
                if sibling.payload_sha256 != digest
                else "载荷一致但事件标识不同"
            )
        )
        row = OfflineEvent(
            device_id=cursor.device_id,
            generation=cursor.generation,
            seq=event.seq,
            event_uid=event.event_uid,
            payload_json=canonical,
            payload_sha256=digest,
            status=ST_CONFLICT,
            conflict_reason=reason,
        )
        db.add(row)
        db.flush()
        return row, True, None

    # 3) 序号早于已定论边界：该序号已有定论，新事件标识不得倒补再生效
    if event.seq <= cursor.confirmed_seq:
        reason = (
            f"序号 {event.seq} 已越过服务端确认边界"
            f"（confirmed_seq={cursor.confirmed_seq}），拒绝倒补"
        )
        row = OfflineEvent(
            device_id=cursor.device_id,
            generation=cursor.generation,
            seq=event.seq,
            event_uid=event.event_uid,
            payload_json=canonical,
            payload_sha256=digest,
            status=ST_CONFLICT,
            conflict_reason=reason,
        )
        db.add(row)
        db.flush()
        return row, True, None

    # 4) 全新事件：持久化为等待态，交由顺序闸门裁定
    row = OfflineEvent(
        device_id=cursor.device_id,
        generation=cursor.generation,
        seq=event.seq,
        event_uid=event.event_uid,
        payload_json=canonical,
        payload_sha256=digest,
        status=ST_WAITING,
    )
    db.add(row)
    db.flush()
    return row, True, None


def _check_business(payload: dict[str, Any], vehicle: Vehicle, station: Station) -> Optional[str]:
    """实体状态前提校验。返回 None 表示通过，否则返回冲突原因。"""
    if payload["soc_after"] <= payload["soc_before"]:
        return "换电后电量应高于换电前电量"
    if station.battery_ready <= 0:
        return f"换电站 {station.name} 暂无满电电池可换（库存为 0）"

    occurred_at = as_naive_utc(datetime.fromisoformat(payload["occurred_at"]))
    # 防倒写时间闸：车辆已有更新的换电（通常是在线交易），旧离线事件不得覆盖较新电量
    if vehicle.last_swapped_at is not None and occurred_at < vehicle.last_swapped_at:
        return (
            f"事件发生时间 {occurred_at.isoformat()} 早于车辆最近一次换电 "
            f"{vehicle.last_swapped_at.isoformat()}，拒绝用旧离线事件倒写较新状态"
        )
    return None


def _apply_event(db: Session, row: OfflineEvent, now: datetime) -> Optional[str]:
    """把单个事件原子落入现有换电链路。返回冲突原因；None 表示成功。

    与在线 /api/swaps 业务效果一致，额外：
    - swapped_at 使用终端事件发生时间；
    - source_kind='offline' 且 source_event_id 唯一，数据库层兜底不重复扣库存；
    - 更新车辆最近换电时间闸，阻断更旧的离线/在线乱序事件。
    """
    payload = json.loads(row.payload_json)
    vehicle = db.get(Vehicle, payload["vehicle_id"])
    station = db.get(Station, payload["station_id"])
    if vehicle is None:
        return f"车辆 {payload['vehicle_id']} 不存在"
    if station is None:
        return f"换电站 {payload['station_id']} 不存在"

    reason = _check_business(payload, vehicle, station)
    if reason is not None:
        return reason

    occurred_at = as_naive_utc(datetime.fromisoformat(payload["occurred_at"]))
    record = SwapRecord(
        vehicle_id=payload["vehicle_id"],
        station_id=payload["station_id"],
        soc_before=payload["soc_before"],
        soc_after=payload["soc_after"],
        swapped_at=occurred_at,
        source_kind="offline",
        source_event_id=row.id,
    )
    db.add(record)
    # 同一事务内更新车辆电量、站点库存、车辆时间闸与事件状态
    vehicle.current_soc = payload["soc_after"]
    vehicle.last_swapped_at = occurred_at
    station.battery_ready -= 1
    row.status = ST_APPLIED
    row.applied_at = now
    db.flush()
    return None


def _pump(db: Session, cursor: DeviceCursor, now: datetime) -> set[int]:
    """顺序闸门：从 confirmed_seq+1 起连续推进。

    返回本次由非 applied 变为已生效（产生换电记录）的事件 id 集合。
    - 缺口（无行）：停止挂起，等终端补齐；
    - waiting：校验前提并原子应用，业务冲突则置 conflict 并阻断；
    - conflict：停止，等人工解决；
    - resolved（补应用或丢弃）：该序号视为有定论，继续推进后续。
    """
    newly_effective: set[int] = set()
    while True:
        next_seq = cursor.confirmed_seq + 1
        row = (
            db.query(OfflineEvent)
            .filter(
                OfflineEvent.device_id == cursor.device_id,
                OfflineEvent.generation == cursor.generation,
                OfflineEvent.seq == next_seq,
            )
            .order_by(OfflineEvent.id)
            .first()
        )
        if row is None:
            break  # 序号缺口
        if row.status == ST_WAITING:
            reason = _apply_event(db, row, now)
            if reason is not None:
                row.status = ST_CONFLICT
                row.conflict_reason = reason
                break
            newly_effective.add(row.id)
        elif row.status == ST_CONFLICT:
            break
        # applied / resolved 均为已有定论，边界前移
        cursor.confirmed_seq = next_seq
    db.flush()
    return newly_effective


def _swap_id_by_event(db: Session, event_ids: set[int]) -> dict[int, int]:
    if not event_ids:
        return {}
    rows = (
        db.query(SwapRecord.source_event_id, SwapRecord.id)
        .filter(SwapRecord.source_event_id.in_(event_ids))
        .all()
    )
    return {event_id: swap_id for event_id, swap_id in rows}


def submit_batch(db: Session, batch: OfflineBatchIn) -> dict[str, Any]:
    """批量接收入口：接收持久化 + 顺序推进在同一事务内完成。

    整批可安全重复发送：重复事件标识返回 duplicate，不重复影响库存；
    本批新事件还可能连带打通此前批次遗留的等待队列。
    """
    now = datetime.utcnow()

    # 事务第一条语句必须是写操作，先取库级写锁再读任何数据
    _acquire_write_lock(db, batch.device_id)
    cursor = _ensure_cursor(db, batch)

    ingested: list[tuple[OfflineEventIn, OfflineEvent, bool, Optional[str]]] = []
    for event in batch.events:
        row, is_new, replay_note = _ingest_one(db, cursor, event)
        ingested.append((event, row, is_new, replay_note))
        cursor.last_seen_seq = max(cursor.last_seen_seq, event.seq)

    # 顺序闸门：连续可应用的事件原子落库（可能推进历史遗留等待事件）
    newly_effective = _pump(db, cursor, now)
    swap_ids = _swap_id_by_event(
        db, newly_effective | {row.id for _, row, _, _ in ingested}
    )

    results: list[OfflineEventResult] = []
    applied_emitted: set[int] = set()
    for event, row, is_new, replay_note in ingested:
        if replay_note is not None:
            # 同一事件标识重传但序号/载荷与首传不一致：拒绝重复定义
            results.append(
                OfflineEventResult(
                    event_uid=event.event_uid, seq=event.seq,
                    status="conflict", reason=replay_note,
                )
            )
            continue

        if row.status == ST_APPLIED:
            # 本次闸门中首次生效（含历史等待事件借本次补缺口生效）-> applied；
            # 同批第二份副本或历史已生效的重传 -> duplicate
            if row.id in newly_effective and row.id not in applied_emitted:
                applied_emitted.add(row.id)
                results.append(
                    OfflineEventResult(
                        event_uid=row.event_uid, seq=row.seq, status="applied",
                        reason="已按序原子应用",
                        swap_record_id=swap_ids.get(row.id),
                    )
                )
            else:
                results.append(
                    OfflineEventResult(
                        event_uid=row.event_uid, seq=row.seq, status="duplicate",
                        reason="事件此前已应用，本次为重传副本",
                        swap_record_id=swap_ids.get(row.id),
                    )
                )
        elif row.status == ST_WAITING:
            results.append(
                OfflineEventResult(
                    event_uid=row.event_uid, seq=row.seq, status="waiting",
                    reason="已接收持久化，等待前序序号补齐",
                )
            )
        elif row.status == ST_CONFLICT:
            results.append(
                OfflineEventResult(
                    event_uid=row.event_uid, seq=row.seq, status="conflict",
                    reason=row.conflict_reason,
                )
            )
        else:  # resolved
            results.append(
                OfflineEventResult(
                    event_uid=row.event_uid, seq=row.seq, status="resolved",
                    reason=f"冲突已人工处理：{row.resolution_action}",
                    swap_record_id=swap_ids.get(row.id),
                )
            )

    db.commit()

    counts = Counter(r.status for r in results)
    return {
        "device_id": cursor.device_id,
        "generation": cursor.generation,
        "confirmed_seq": cursor.confirmed_seq,
        "applied_count": counts.get("applied", 0),
        "duplicate_count": counts.get("duplicate", 0),
        "waiting_count": counts.get("waiting", 0),
        "conflict_count": counts.get("conflict", 0) + counts.get("resolved", 0),
        "results": results,
    }


def get_cursor_view(db: Session, device_id: str) -> Optional[dict[str, Any]]:
    """终端查询确认范围：哪些缓存可清理、缺口在哪、是否有冲突待处理。"""
    cursor = _get_cursor(db, device_id)
    if cursor is None:
        return None

    existing_seqs = {
        s[0]
        for s in db.query(OfflineEvent.seq)
        .filter(
            OfflineEvent.device_id == device_id,
            OfflineEvent.generation == cursor.generation,
        )
        .all()
    }
    missing = [
        s
        for s in range(cursor.confirmed_seq + 1, cursor.last_seen_seq + 1)
        if s not in existing_seqs
    ][:200]

    has_open_conflict = (
        db.query(OfflineEvent.id)
        .filter(
            OfflineEvent.device_id == device_id,
            OfflineEvent.status == ST_CONFLICT,
        )
        .first()
        is not None
    )

    return {
        "device_id": cursor.device_id,
        "station_id": cursor.station_id,
        "generation": cursor.generation,
        "generation_started_at": cursor.generation_started_at,
        "confirmed_seq": cursor.confirmed_seq,
        "last_seen_seq": cursor.last_seen_seq,
        "missing_seqs": missing,
        "has_open_conflict": has_open_conflict,
        "updated_at": cursor.updated_at,
    }


def resolve_event(
    db: Session, event_id: int, action: str, reason: str, username: str,
    now: Optional[datetime] = None,
) -> OfflineEvent:
    """人工解决冲突/挂起事件。

    必须留下处理原因；原始载荷与 SHA-256 摘要在接收时已固化，不可修改。
    action='apply'：校验前提后补应用（同样走原子链路，source_event_id 唯一兜底）；
    action='drop' ：确认丢弃，序号视为有定论，闸门自动继续后续可推进事件。
    """
    now = now or datetime.utcnow()
    row = db.get(OfflineEvent, event_id)
    if row is None:
        raise KeyError("事件不存在")
    if row.status not in (ST_CONFLICT, ST_WAITING):
        raise ValueError("仅冲突或挂起等待中的事件可以人工处理")

    if (
        db.query(SwapRecord.id)
        .filter(SwapRecord.source_event_id == row.id)
        .first()
        is not None
    ):
        raise ValueError("该事件已产生换电记录，不能重复处理")

    cursor = _get_cursor(db, row.device_id)
    current_gen = cursor.generation if cursor is not None else None
    head_seq = (cursor.confirmed_seq + 1) if cursor is not None else None

    if row.generation != current_gen:
        # 旧会话代次的死信事件：序号体系已作废，只能确认丢弃，不能补应用
        if action == "apply":
            raise ValueError("旧会话代次的事件不能补应用，只能确认丢弃")
    elif row.seq != head_seq:
        if row.seq < head_seq:
            raise ValueError("该序号已有定论，无需人工处理")
        raise ValueError(
            f"前序序号 {head_seq} 尚未定论，不能越过顺序前提处理序号 {row.seq}"
        )

    if action == "apply":
        apply_reason = _apply_event(db, row, now)
        if apply_reason is not None:
            row.status = ST_CONFLICT
            row.conflict_reason = (
                f"{row.conflict_reason or ''} | 人工尝试应用失败：{apply_reason}"
            ).strip(" |")
            db.commit()
            raise ValueError(apply_reason)
        row.resolution_action = "applied"
    else:
        row.resolution_action = "dropped"

    row.status = ST_RESOLVED
    row.resolved_by = username
    row.resolved_at = now
    # 审计：人工原因追加保存，原始冲突原因与载荷摘要保持不变
    row.conflict_reason = (
        f"{row.conflict_reason or ''} | 人工处理（{username}）：{reason}"
    ).strip(" |")
    db.flush()

    # 当前代次队首定论后，自动续推该设备队列（解除阻断后继续后续可推进记录）
    if cursor is not None and row.generation == cursor.generation:
        _pump(db, cursor, now)

    db.commit()
    db.refresh(row)
    return row


def pump_all_devices(db: Session) -> int:
    """服务重启后兜底：对所有设备续推等待队列。返回有推进的设备数。

    待处理队列全部持久化在 offline_events 中，重启不丢；缺口若已被此前
    （服务不可用期间无法提交，故主要指人工修复/补录后）补齐，此处自动续推。
    """
    now = datetime.utcnow()
    moved = 0
    for cursor in db.query(DeviceCursor).all():
        before = cursor.confirmed_seq
        _pump(db, cursor, now)
        if cursor.confirmed_seq != before:
            moved += 1
    db.commit()
    return moved


def reconcile(db: Session) -> dict[str, Any]:
    """对账：证明每个接受事件至多影响库存一次、旧离线事件未倒写在线交易。"""
    total_by_status = dict(
        db.query(OfflineEvent.status, text("count(*)"))
        .group_by(OfflineEvent.status)
        .all()
    )

    applied_ids = {
        r[0] for r in db.query(OfflineEvent.id).filter(
            OfflineEvent.status == ST_APPLIED
        ).all()
    }
    resolved_applied_ids = {
        r[0] for r in db.query(OfflineEvent.id).filter(
            OfflineEvent.status == ST_RESOLVED,
            OfflineEvent.resolution_action == "applied",
        ).all()
    }
    effective_ids = applied_ids | resolved_applied_ids

    # 每个生效事件应恰好对应一条离线换电记录
    event_to_count: dict[int, int] = {}
    if effective_ids:
        for event_id, cnt in (
            db.query(SwapRecord.source_event_id, text("count(*)"))
            .filter(SwapRecord.source_event_id.isnot(None))
            .group_by(SwapRecord.source_event_id)
            .all()
        ):
            event_to_count[event_id] = cnt
    duplicate_events = [eid for eid, cnt in event_to_count.items() if cnt > 1]
    applied_without_swap = sorted(effective_ids - set(event_to_count))
    swap_without_event = sorted(set(event_to_count) - effective_ids)

    # 防倒写核查：离线事件应用之前，若该车辆已存在发生时间更晚的在线交易，
    # 即为一次倒写（时间闸正确生效时该集合必为空）。
    backwrite_risks: list[dict[str, Any]] = []
    if effective_ids:
        offline_rows = (
            db.query(OfflineEvent)
            .filter(OfflineEvent.id.in_(effective_ids))
            .all()
        )
        for row in offline_rows:
            payload = json.loads(row.payload_json)
            occurred_at = as_naive_utc(
                datetime.fromisoformat(payload["occurred_at"])
            )
            blocking = (
                db.query(SwapRecord)
                .filter(
                    SwapRecord.vehicle_id == payload["vehicle_id"],
                    SwapRecord.source_kind == "online",
                    SwapRecord.swapped_at > occurred_at,
                    SwapRecord.swapped_at < row.applied_at,
                )
                .first()
            )
            if blocking is not None:
                backwrite_risks.append(
                    {
                        "event_id": row.id,
                        "vehicle_id": payload["vehicle_id"],
                        "offline_occurred_at": occurred_at.isoformat(),
                        "blocking_swap_id": blocking.id,
                    }
                )

    ok = (
        not applied_without_swap
        and not swap_without_event
        and not duplicate_events
        and not backwrite_risks
    )
    return {
        "ok": ok,
        "events_total": sum(total_by_status.values()),
        "events_by_status": total_by_status,
        "effective_event_count": len(effective_ids),
        "offline_swap_record_count": len(event_to_count),
        "applied_without_swap": applied_without_swap,
        "swap_without_event": swap_without_event,
        "duplicate_application_events": duplicate_events,
        "backwrite_risks": backwrite_risks,
    }
