"""数据库模型。

业务主题：新能源物流车换电站运营管理。
"""
from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from .database import Base


class User(Base):
    """后台用户（本平台只有 admin 一个管理员角色）。"""

    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(64), unique=True, nullable=False, index=True)
    password_hash = Column(String(256), nullable=False)
    display_name = Column(String(64), nullable=False, default="管理员")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class Station(Base):
    """换电站。"""

    __tablename__ = "stations"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(128), nullable=False)
    address = Column(String(256), nullable=False, default="")
    # 电池仓位总数与当前满电可换电池数
    slot_total = Column(Integer, nullable=False, default=0)
    battery_ready = Column(Integer, nullable=False, default=0)
    # 运营状态：running 运营中 / maintenance 维护中 / offline 离线
    status = Column(String(16), nullable=False, default="running")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    swaps = relationship("SwapRecord", back_populates="station")


class Vehicle(Base):
    """新能源物流车。"""

    __tablename__ = "vehicles"

    id = Column(Integer, primary_key=True, index=True)
    plate = Column(String(32), unique=True, nullable=False, index=True)
    model = Column(String(64), nullable=False, default="")
    battery_capacity = Column(Float, nullable=False, default=100.0)  # kWh
    current_soc = Column(Float, nullable=False, default=100.0)  # 0-100 百分比
    # 当前电量最近一次被写入的业务时间：旧离线事件不得倒写较新的车辆电量
    soc_updated_at = Column(DateTime, nullable=True)
    # 状态：idle 空闲 / running 运营 / charging 换电中 / fault 故障
    status = Column(String(16), nullable=False, default="idle")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    swaps = relationship("SwapRecord", back_populates="vehicle")


class SwapRecord(Base):
    """换电记录。"""

    __tablename__ = "swap_records"

    id = Column(Integer, primary_key=True, index=True)
    vehicle_id = Column(Integer, ForeignKey("vehicles.id"), nullable=False, index=True)
    station_id = Column(Integer, ForeignKey("stations.id"), nullable=False, index=True)
    soc_before = Column(Float, nullable=False, default=0.0)
    soc_after = Column(Float, nullable=False, default=100.0)
    swapped_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    # 由终端批量同步事件落账时，记录终端稳定事件标识；唯一约束在数据库层保证
    # 同一接受事件至多产生一条换电记录（即至多扣减一次库存）。在线手工登记为 NULL。
    sync_event_id = Column(String(128), unique=True, nullable=True, index=True)

    vehicle = relationship("Vehicle", back_populates="swaps")
    station = relationship("Station", back_populates="swaps")


# ---------------------------------------------------------------------------
# 离线事件批量同步
# ---------------------------------------------------------------------------

# 事件生命周期：
#   new       刚落库、尚未尝试（瞬态，不对终端承诺）
#   waiting   前序序号缺口，等待补齐
#   applied   已原子落入换电链路（终态）
#   conflict  业务前提冲突（如无可用电池），等待人工裁决（终态，需解除后才推进）
#   rejected  永久无效（载荷错误 / 实体不存在 / 旧会话代次），系统或人工终结（终态）
#   duplicate 老事件的重传副本（终态，幂等无副作用）
#   skipped_gap 经人工确认跳过的缺号墓碑（终态）
EVENT_TRANSIENT_STATUSES = ("new", "waiting")
EVENT_FINAL_STATUSES = ("applied", "conflict", "rejected", "duplicate", "skipped_gap")


class DeviceCursor(Base):
    """终端设备游标：每个终端一行，记录会话代次与连续确认到的序号。"""

    __tablename__ = "device_cursors"

    device_id = Column(String(64), primary_key=True)
    # 设备最近一次上报所属的会话代次（设备重置后轮转，单调递增）
    session_generation = Column(Integer, nullable=False, default=1)
    # 当前代次内已连续终结（applied/rejected/skipped_gap）到的最大序号
    applied_seq = Column(Integer, nullable=False, default=0)
    # 服务端见过的最大序号（不要求连续，仅用于观测）
    max_seen_seq = Column(Integer, nullable=False, default=0)
    # 队首最早挂起时间（waiting/conflict 阻塞起点），便于运维发现积压
    blocked_since = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class SyncEvent(Base):
    """终端批量同步的换电事件（含接收与处理状态）。"""

    __tablename__ = "sync_events"
    __table_args__ = (
        UniqueConstraint(
            "device_id", "session_generation", "seq", name="uq_sync_event_coordinates"
        ),
        Index("ix_sync_events_device_status", "device_id", "session_generation", "status"),
    )

    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(String(64), nullable=False, index=True)
    session_generation = Column(Integer, nullable=False)
    seq = Column(Integer, nullable=False)
    # 终端生成的稳定事件标识（重传不变），全局唯一、供终端去重与对账
    event_id = Column(String(128), unique=True, nullable=False, index=True)
    event_type = Column(String(64), nullable=False, default="swap_completed")
    # 原始信封（JSON 原文）与规范化 sha256 摘要，冲突处理时留存证据
    raw_payload = Column(Text, nullable=False, default="{}")
    payload_hash = Column(String(64), nullable=False)
    # 事件在终端实际发生时间（载荷内 occurred_at，缺省取首次接收时间）
    occurred_at = Column(DateTime, nullable=False)

    status = Column(String(16), nullable=False, default="new", index=True)
    # 状态说明 / 冲突原因 / 旧值回写抑制等处理注记
    detail = Column(String(512), nullable=True)
    # 落账后关联的换电记录
    swap_record_id = Column(Integer, ForeignKey("swap_records.id"), nullable=True)
    # 该事件落账时是否因事件时间过旧而抑制了车辆电量回写（库存仍只扣一次）
    soc_write_suppressed = Column(Boolean, nullable=False, default=False)

    # 人工裁决信息（冲突处理必须留原因；系统终结同样记录原因）
    resolution_reason = Column(String(512), nullable=True)
    resolved_by = Column(String(64), nullable=True)
    resolved_at = Column(DateTime, nullable=True)

    first_seen = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_seen = Column(DateTime, default=datetime.utcnow, nullable=False)
    applied_at = Column(DateTime, nullable=True)

    swap_record = relationship("SwapRecord", foreign_keys=[swap_record_id])


class SyncRejectedDelivery(Base):
    """协议级拒收留痕：坐标冲突、event_id 复用等无法占用事件表唯一坐标的送达。

    保证「每一次接收都有持久化结果」，重传时同样幂等命中。
    """

    __tablename__ = "sync_rejected_deliveries"

    id = Column(Integer, primary_key=True)
    device_id = Column(String(64), nullable=False, index=True)
    session_generation = Column(Integer, nullable=False)
    seq = Column(Integer, nullable=False)
    event_id = Column(String(128), unique=True, nullable=False, index=True)
    raw_payload = Column(Text, nullable=False, default="{}")
    payload_hash = Column(String(64), nullable=False)
    reason = Column(String(512), nullable=False)
    first_seen = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_seen = Column(DateTime, default=datetime.utcnow, nullable=False)
