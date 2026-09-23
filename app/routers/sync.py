"""终端离线事件批量同步路由（需登录）。

提供：
- POST /api/sync/batches           终端批量提交离线换电事件（可安全重发）
- GET  /api/sync/devices/{id}      终端查询服务端确认范围与缺口
- GET  /api/sync/events            后台查询事件队列（冲突处理台）
- GET  /api/sync/events/{id}       事件详情（含原始载荷摘要）
- POST /api/sync/events/{id}/resolve 人工解决冲突（必须填写原因）
- GET  /api/sync/reconcile         对账结果（幂等性与防倒写证明）
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ..auth import get_current_user
from ..database import get_db
from ..models import OfflineEvent, User
from ..schemas import (
    ConflictResolveIn,
    DeviceCursorOut,
    OfflineBatchIn,
    OfflineBatchOut,
    OfflineEventOut,
)
from ..services import sync

router = APIRouter(prefix="/api/sync", tags=["离线同步"], dependencies=[Depends(get_current_user)])


@router.post("/batches", response_model=OfflineBatchOut)
def submit_batch(payload: OfflineBatchIn, db: Session = Depends(get_db)):
    # 同批内序号/事件标识自身重复先做快速校验，持久层仍有唯一约束兜底
    uids = [e.event_uid for e in payload.events]
    if len(set(uids)) != len(uids):
        raise HTTPException(status_code=422, detail="同一批次内存在重复的事件标识")
    seqs = [(e.seq, e.event_uid) for e in payload.events]
    seq_only = [s for s, _ in seqs]
    if len(set(seq_only)) != len(seq_only):
        raise HTTPException(status_code=422, detail="同一批次内存在重复序号")
    try:
        return sync.submit_batch(db, payload)
    except sync.StaleGenerationError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": str(exc),
                "current_generation": exc.current_generation,
                "current_generation_started_at": exc.current_started_at.isoformat(),
            },
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@router.get("/devices/{device_id}", response_model=DeviceCursorOut)
def get_device_cursor(device_id: str, db: Session = Depends(get_db)):
    view = sync.get_cursor_view(db, device_id)
    if view is None:
        raise HTTPException(status_code=404, detail="设备尚未注册游标，请先提交一批事件")
    return view


@router.get("/events", response_model=list[OfflineEventOut])
def list_events(
    device_id: str | None = None,
    status_filter: str | None = None,
    db: Session = Depends(get_db),
):
    query = db.query(OfflineEvent)
    if device_id:
        query = query.filter(OfflineEvent.device_id == device_id)
    if status_filter:
        if status_filter not in ("waiting", "applied", "conflict", "resolved"):
            raise HTTPException(status_code=422, detail="非法的状态过滤值")
        query = query.filter(OfflineEvent.status == status_filter)
    return query.order_by(OfflineEvent.device_id, OfflineEvent.generation, OfflineEvent.seq).all()


@router.get("/events/{event_id}", response_model=OfflineEventOut)
def get_event(event_id: int, db: Session = Depends(get_db)):
    row = db.get(OfflineEvent, event_id)
    if row is None:
        raise HTTPException(status_code=404, detail="事件不存在")
    return row


@router.post("/events/{event_id}/resolve", response_model=OfflineEventOut)
def resolve_event(
    event_id: int,
    payload: ConflictResolveIn,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    try:
        return sync.resolve_event(
            db, event_id, payload.action, payload.reason, user.username
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="事件不存在")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@router.post("/pump", status_code=status.HTTP_200_OK)
def pump_all(db: Session = Depends(get_db)):
    """手动触发全设备续推（服务重启后也会在启动时自动执行一次）。"""
    moved = sync.pump_all_devices(db)
    return {"devices_advanced": moved}


@router.get("/reconcile")
def reconcile(db: Session = Depends(get_db)):
    return sync.reconcile(db)
