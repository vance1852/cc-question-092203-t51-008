"""Pydantic 数据模型（请求体与响应体）。"""
from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


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
# 事件接收结果四态（外加 resolved 表示冲突已人工定论）
EventStatus = Literal["applied", "duplicate", "waiting", "conflict", "resolved"]


class OfflineEventIn(BaseModel):
    """终端缓存的单笔换电事件。"""

    # 稳定事件标识：终端首次产生时生成（建议 UUID），重传整批必须保持不变
    event_uid: str = Field(..., min_length=1, max_length=64)
    # 同一会话代次内严格递增的序号，从 1 开始
    seq: int = Field(..., ge=1)
    vehicle_id: int
    station_id: int
    soc_before: float = Field(..., ge=0, le=100)
    soc_after: float = Field(..., ge=0, le=100)
    # 事件在终端实际发生的时间（ISO8601 字符串），用于还原 swapped_at
    occurred_at: datetime


class OfflineBatchIn(BaseModel):
    """终端批量同步请求体：同一批可安全重复发送。"""

    device_id: str = Field(..., min_length=1, max_length=64)
    # 会话代次标识（设备重置/重新建档后必须更换）及其起始时间
    generation: str = Field(..., min_length=1, max_length=64)
    generation_started_at: datetime
    # 终端绑定的站点（可选，仅在首次注册游标时记录）
    station_id: Optional[int] = None
    events: list[OfflineEventIn] = Field(..., min_length=1, max_length=1000)


class OfflineEventResult(BaseModel):
    """单笔事件的持久化接收结果。"""

    event_uid: str
    seq: int
    # applied 已应用 / duplicate 重复 / waiting 等待前序 / conflict 业务冲突
    status: EventStatus
    # 冲突或无法应用的说明（conflict 时必填语义）
    reason: Optional[str] = None
    # 应用成功后对应的换电记录 id
    swap_record_id: Optional[int] = None


class OfflineBatchOut(BaseModel):
    """批量同步响应：总体结论 + 逐笔结果 + 服务端确认范围。"""

    device_id: str
    generation: str
    # 服务端已连续有定论的最大序号：终端可安全清理 seq <= 该值的本地缓存
    confirmed_seq: int
    # 本批次中新应用的事件数（duplicate 不计）
    applied_count: int
    duplicate_count: int
    waiting_count: int
    conflict_count: int
    results: list[OfflineEventResult]


class DeviceCursorOut(BaseModel):
    """设备游标与确认范围（供终端查询，决定哪些缓存可清理 / 缺口在哪）。"""

    device_id: str
    station_id: Optional[int] = None
    generation: str
    generation_started_at: datetime
    confirmed_seq: int
    last_seen_seq: int
    # 尚缺的序号列表（waiting 队列引用的前序缺口，最多返回前 200 个）
    missing_seqs: list[int]
    # 是否存在待人工处理的冲突
    has_open_conflict: bool
    updated_at: datetime

    model_config = {"from_attributes": True}


class ConflictResolveIn(BaseModel):
    """人工解决冲突（或对挂起事件做人工定论）。"""

    # 处理动作：apply 忽略前序缺口强制应用 / drop 确认丢弃该事件
    action: Literal["apply", "drop"]
    # 必填：人工处理原因（用于审计）
    reason: str = Field(..., min_length=1, max_length=500)


class OfflineEventOut(BaseModel):
    """离线事件详情（冲突处理台使用，含原始载荷摘要）。"""

    id: int
    device_id: str
    generation: str
    seq: int
    event_uid: str
    payload_json: str
    payload_sha256: str
    status: EventStatus
    conflict_reason: Optional[str] = None
    resolved_by: Optional[str] = None
    resolved_at: Optional[datetime] = None
    resolution_action: Optional[str] = None
    swap_record_id: Optional[int] = None
    received_at: datetime
    applied_at: Optional[datetime] = None

    model_config = {"from_attributes": True}
