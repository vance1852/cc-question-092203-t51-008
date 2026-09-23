"""终端离线事件批量同步路由（需登录）。

终端在断网恢复后可安全重复发送整批数据；服务端幂等接收、按序应用、
持久化全部接收结果，并提供游标查询、人工裁决与对账能力。
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ..auth import get_current_user
from ..database import get_db
from ..models import DeviceCursor, User
from ..schemas import (
    ConflictResolveRequest,
    DeviceCursorOut,
    ReconcileReport,
    SkipGapRequest,
    SyncBatchRequest,
    SyncBatchResponse,
    SyncEventOut,
)
from ..services import sync_service

router = APIRouter(prefix="/api/sync", tags=["离线同步"], dependencies=[Depends(get_current_user)])


@router.post("/batch", response_model=SyncBatchResponse)
def upload_batch(payload: SyncBatchRequest, db: Session = Depends(get_db)):
    """批量同步入口：同一批可任意重发，结果逐事件返回。"""
    items = [event.model_dump() for event in payload.events]
    try:
        return sync_service.ingest_batch(
            db, payload.device_id, payload.session_generation, items
        )
    except Exception:  # noqa: BLE001 —— 任何异常都回滚，绝不留半截状态
        db.rollback()
        raise


@router.get("/devices", response_model=list[DeviceCursorOut])
def list_devices(db: Session = Depends(get_db)):
    return db.query(DeviceCursor).order_by(DeviceCursor.device_id).all()


@router.get("/devices/{device_id}/cursor", response_model=DeviceCursorOut)
def get_device_cursor(device_id: str, db: Session = Depends(get_db)):
    """查询设备游标（会话代次、连续确认范围、积压起点），终端据此清理本地缓存。"""
    try:
        return sync_service.get_cursor(db, device_id)
    except LookupError:
        raise HTTPException(status_code=404, detail="设备尚未同步过任何事件")


@router.get("/devices/{device_id}/events", response_model=list[SyncEventOut])
def list_device_events(
    device_id: str,
    generation: int | None = None,
    status_filter: str | None = None,
    db: Session = Depends(get_db),
):
    return sync_service.list_events(db, device_id, generation, status_filter)


@router.post(
    "/devices/{device_id}/events/{event_id}/resolve",
    response_model=SyncEventOut,
)
def resolve_event(
    device_id: str,
    event_id: str,
    payload: ConflictResolveRequest,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """人工裁决业务冲突：强制应用或驳回，必须填写原因并留存原始载荷摘要。"""
    try:
        ev = sync_service.resolve_conflict(
            db,
            device_id=device_id,
            event_id=event_id,
            action=payload.action,
            reason=payload.reason,
            username=user.username,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except sync_service.ConflictStillBusy as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return ev


@router.post("/devices/{device_id}/skip-gap", response_model=SyncEventOut)
def skip_gap(
    device_id: str,
    payload: SkipGapRequest,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """人工确认缺号永久丢失，登记墓碑后自动续推后续事件。"""
    try:
        marker = sync_service.skip_gap(
            db,
            device_id=device_id,
            seq=payload.seq,
            reason=payload.reason,
            username=user.username,
            generation=payload.session_generation,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return marker


@router.get("/reconcile", response_model=ReconcileReport)
def reconcile(device_id: str | None = None, db: Session = Depends(get_db)):
    """对账报告：库存至多一次、在线电量不被旧离线事件倒写、游标连续性。"""
    return sync_service.reconcile(db, device_id)
