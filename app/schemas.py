"""Pydantic 数据模型（请求体与响应体）。"""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, model_validator


# ---------- 认证 ----------
class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserOut(BaseModel):
    id: int
    username: str
    display_name: str

    model_config = {"from_attributes": True}


# ---------- 换电站 ----------
class StationBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    address: str = ""
    slot_total: int = Field(0, ge=0)
    battery_ready: int = Field(0, ge=0)
    status: str = Field("running", pattern="^(running|maintenance|offline)$")


class StationCreate(StationBase):
    pass


class StationUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=128)
    address: Optional[str] = None
    slot_total: Optional[int] = Field(None, ge=0)
    battery_ready: Optional[int] = Field(None, ge=0)
    status: Optional[str] = Field(None, pattern="^(running|maintenance|offline)$")


class StationOut(StationBase):
    id: int
    created_at: datetime

    model_config = {"from_attributes": True}


# ---------- 车辆 ----------
class VehicleBase(BaseModel):
    plate: str = Field(..., min_length=1, max_length=32)
    model: str = ""
    battery_capacity: float = Field(100.0, gt=0)
    current_soc: float = Field(100.0, ge=0, le=100)
    status: str = Field("idle", pattern="^(idle|running|charging|fault)$")


class VehicleCreate(VehicleBase):
    pass


class VehicleUpdate(BaseModel):
    plate: Optional[str] = Field(None, min_length=1, max_length=32)
    model: Optional[str] = None
    battery_capacity: Optional[float] = Field(None, gt=0)
    current_soc: Optional[float] = Field(None, ge=0, le=100)
    status: Optional[str] = Field(None, pattern="^(idle|running|charging|fault)$")


class VehicleOut(VehicleBase):
    id: int
    created_at: datetime

    model_config = {"from_attributes": True}


# ---------- 换电记录 ----------
class SwapCreate(BaseModel):
    vehicle_id: int
    station_id: int
    soc_before: float = Field(..., ge=0, le=100)
    soc_after: float = Field(100.0, ge=0, le=100)


class SwapOut(BaseModel):
    id: int
    vehicle_id: int
    station_id: int
    soc_before: float
    soc_after: float
    swapped_at: datetime
    vehicle_plate: Optional[str] = None
    station_name: Optional[str] = None

    model_config = {"from_attributes": True}


# ---------- 仪表盘 ----------
class DashboardStats(BaseModel):
    station_total: int
    station_running: int
    vehicle_total: int
    vehicle_fault: int
    swap_today: int
    battery_ready_total: int


# ---------- 终端离线事件批量同步 ----------
class OfflineEventIn(BaseModel):
    """终端缓存的单个换电事件（重传时 event_id / seq 必须保持不变）。"""

    event_id: str = Field(..., min_length=1, max_length=128, description="终端生成的稳定事件标识")
    seq: int = Field(..., ge=1, description="会话内单调递增序号，从 1 开始")
    event_type: str = Field("swap_completed", min_length=1, max_length=64)
    occurred_at: Optional[datetime] = Field(None, description="事件在终端实际发生时间；缺省取服务端首次接收时间")
    vehicle_id: Optional[int] = None
    vehicle_plate: Optional[str] = Field(None, max_length=32)
    station_id: int
    soc_before: float = Field(..., ge=0, le=100)
    soc_after: float = Field(..., ge=0, le=100)

    @model_validator(mode="after")
    def _require_vehicle(self):
        if not self.vehicle_id and not self.vehicle_plate:
            raise ValueError("vehicle_id 与 vehicle_plate 至少提供一个")
        return self


class SyncBatchRequest(BaseModel):
    """一批离线事件（终端可安全重复发送同一批）。"""

    device_id: str = Field(..., min_length=1, max_length=64)
    session_generation: int = Field(..., ge=1, description="会话代次；设备重置后轮转并递增")
    events: list[OfflineEventIn] = Field(..., min_length=1, max_length=500)


class EventResult(BaseModel):
    """批内单个事件的接收/处理结果。"""

    event_id: str
    seq: int
    status: str = Field(..., description="applied 已应用 / duplicate 重复送达 / waiting 等待前序 / conflict 业务冲突 / rejected 永久无效 / skipped_gap 缺号跳过")
    detail: Optional[str] = None
    swap_record_id: Optional[int] = None
    payload_hash: str


class SyncBatchResponse(BaseModel):
    device_id: str
    session_generation: int
    # 连续确认序号：[1, confirmed_seq] 均已终结，终端可据此清理本地缓存
    confirmed_seq: int
    max_seen_seq: int
    blocked_since: Optional[datetime] = None
    results: list[EventResult]


class DeviceCursorOut(BaseModel):
    device_id: str
    session_generation: int
    applied_seq: int
    max_seen_seq: int
    blocked_since: Optional[datetime]
    updated_at: datetime

    model_config = {"from_attributes": True}


class SyncEventOut(BaseModel):
    id: int
    device_id: str
    session_generation: int
    seq: int
    event_id: str
    event_type: str
    raw_payload: str
    payload_hash: str
    occurred_at: datetime
    status: str
    detail: Optional[str]
    swap_record_id: Optional[int]
    soc_write_suppressed: bool = False
    resolution_reason: Optional[str]
    resolved_by: Optional[str]
    resolved_at: Optional[datetime]
    first_seen: datetime
    last_seen: datetime
    applied_at: Optional[datetime]

    model_config = {"from_attributes": True}


class ConflictResolveRequest(BaseModel):
    """人工裁决：必须填写原因。"""

    action: str = Field(..., pattern="^(force_apply|reject)$")
    reason: str = Field(..., min_length=1, max_length=512)


class SkipGapRequest(BaseModel):
    """人工确认某个缺失序号永久不会到达，跳过以解队。"""

    seq: int = Field(..., ge=1)
    reason: str = Field(..., min_length=1, max_length=512)
    session_generation: Optional[int] = None


class ReconcileDevice(BaseModel):
    device_id: str
    session_generation: int
    applied_seq: int
    max_seen_seq: int
    blocked_since: Optional[datetime]
    counts: dict[str, int]


class ReconcileReport(BaseModel):
    scope_device_id: Optional[str]
    # 各状态事件数
    status_counts: dict[str, int]
    # 已应用（已扣库存）事件数
    applied_count: int
    # 换电记录表中由离线同步落账的行数
    offline_swap_row_count: int
    # 电量回写被抑制（旧事件不覆盖较新车辆状态）的事件数
    soc_write_suppressed_count: int
    # 对账不变量
    every_applied_has_swap: bool
    inventory_effect_at_most_once: bool
    # 在线交易电量是否未被旧离线事件倒写
    online_not_overwritten_by_stale: bool
    overwrite_violations: list[str] = []
    devices: list[ReconcileDevice]
    events: list[SyncEventOut]
