"""仪表盘统计路由（需登录）。"""
from datetime import datetime

from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlalchemy.orm import Session

from ..auth import get_current_user
from ..database import get_db
from ..models import Station, SwapRecord, Vehicle
from ..schemas import DashboardStats

router = APIRouter(prefix="/api/dashboard", tags=["仪表盘"], dependencies=[Depends(get_current_user)])


@router.get("/stats", response_model=DashboardStats)
def stats(db: Session = Depends(get_db)):
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    return DashboardStats(
        station_total=db.query(Station).count(),
        station_running=db.query(Station).filter(Station.status == "running").count(),
        vehicle_total=db.query(Vehicle).count(),
        vehicle_fault=db.query(Vehicle).filter(Vehicle.status == "fault").count(),
        swap_today=db.query(SwapRecord).filter(SwapRecord.swapped_at >= today_start).count(),
        battery_ready_total=db.query(func.coalesce(func.sum(Station.battery_ready), 0)).scalar() or 0,
    )
