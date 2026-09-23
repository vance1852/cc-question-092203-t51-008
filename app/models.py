"""数据库模型。

业务主题：新能源物流车换电站运营管理。
"""
from datetime import datetime
from typing import Optional

from sqlalchemy import (
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
    # 状态：idle 空闲 / running 运营 / charging 换电中 / fault 故障
    status = Column(String(16), nullable=False, default="idle")
    # 最近一次换电（成功应用）的业务发生时间，作为防旧事件倒写的时间闸
    last_swapped_at = Column(DateTime, nullable=True)
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
    # 来源：online 在线逐笔登记 / offline 离线批量同步应用
    source_kind = Column(String(16), nullable=False, default="online")
    # 离线事件唯一溯源：一个离线事件最多生成一条换电记录（唯一约束兜底幂等）
    source_event_id = Column(Integer, ForeignKey("offline_events.id"), nullable=True, unique=True)

    vehicle = relationship("Vehicle", back_populates="swaps")
    station = relationship("Station", back_populates="swaps")


class DeviceCursor(Base):
    """设备游标：每个换电终端一行，记录会话代次与服务端确认范围。

    终端侧约定：
    - device_id 为终端稳定硬件标识；
    - generation 为设备会话代次（设备重置/重新建档后必须更换，建议用 UUID），
      generation_started_at 为该代次起始时间，服务端据此识别新代次与过期代次；
    - seq 在同一代次内从 1 开始严格递增，不跳号由服务端监督。
    """

    __tablename__ = "device_cursors"

    device_id = Column(String(64), primary_key=True)
    station_id = Column(Integer, ForeignKey("stations.id"), nullable=True)
    # 终端当前生效的会话代次及其起始时间
    generation = Column(String(64), nullable=False)
    generation_started_at = Column(DateTime, nullable=False)
    # 该代次内已连续有定论（applied 或人工 dropped）的最大序号，0 表示尚无定论
    confirmed_seq = Column(Integer, nullable=False, default=0)
    # 终端自报的最大序号（可能大于 confirmed_seq，中间存在缺口或冲突）
    last_seen_seq = Column(Integer, nullable=False, default=0)
    registered_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class OfflineEvent(Base):
    """离线换电事件：终端断网缓存、恢复后批量重传的事件记录。

    持久化状态（status）：
    - waiting   已接收落库，存在序号缺口或等待顺序闸门，挂起中；
    - applied   顺序与实体状态前提均满足，已原子落入换电链路；
    - conflict  业务冲突（无可用电池/实体不存在/同序号事件标识不一致/旧事件倒写等）；
    - resolved  冲突经人工处理后已定论（resolution_action: applied 补应用 / dropped 丢弃）。

    注：duplicate（重传副本）只是接口响应态，不产生第二种数据库状态——
    重传事件直接引用首传行的状态。

    幂等性由 (device_id, event_uid) 唯一约束保证。
    (device_id, generation, seq) 故意不加唯一约束：同序号不同事件标识的
    冲突载荷也必须落库留审计，顺序归属由应用层按最早到达行裁定。
    """

    __tablename__ = "offline_events"
    __table_args__ = (
        UniqueConstraint("device_id", "event_uid", name="uq_offline_event_uid"),
        # 注意：(device_id, generation, seq) 不加唯一约束——同序号不同事件标识的
        # 冲突载荷也必须落库留审计；顺序归属由服务端在应用层按最早到达行裁定。
        Index("ix_offline_event_gen_seq", "device_id", "generation", "seq"),
        Index("ix_offline_event_device_status", "device_id", "status"),
    )

    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(String(64), nullable=False, index=True)
    generation = Column(String(64), nullable=False)
    seq = Column(Integer, nullable=False)
    # 终端生成的稳定事件标识（重传整批时保持不变）
    event_uid = Column(String(64), nullable=False)
    # 业务载荷（vehicle_id/station_id/soc_before/soc_after/事件时间）原样 JSON
    payload_json = Column(Text, nullable=False)
    payload_sha256 = Column(String(64), nullable=False)

    status = Column(String(16), nullable=False, default="waiting", index=True)
    # 冲突/解决原因（人工解决时必填）
    conflict_reason = Column(Text, nullable=True)
    # 人工处理人用户名与处理时间
    resolved_by = Column(String(64), nullable=True)
    resolved_at = Column(DateTime, nullable=True)
    # 处理动作：applied 补应用 / dropped 确认丢弃
    resolution_action = Column(String(16), nullable=True)

    received_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    applied_at = Column(DateTime, nullable=True)

    @property
    def swap_record_id(self) -> Optional[int]:  # type: ignore[override]
        """该事件应用后生成的换电记录 id（未应用为 None）。"""
        record = self.swap_record
        return record.id if record is not None else None

    swap_record = relationship(
        "SwapRecord",
        uselist=False,
        viewonly=True,
        primaryjoin="OfflineEvent.id == SwapRecord.source_event_id",
        foreign_keys="SwapRecord.source_event_id",
    )
