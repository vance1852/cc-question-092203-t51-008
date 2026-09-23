"""应用入口。

新能源物流车换电站运营管理平台 —— 纯后端 API 服务。
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .database import SessionLocal
from .routers import auth, dashboard, stations, swaps, sync, vehicles
from .seed import init_db
from .services import sync as sync_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时初始化数据库（建表 + 种子数据）
    init_db()
    # 重启不丢待处理队列：对所有设备尝试续推，补齐过缺口的等待事件自动应用
    db = SessionLocal()
    try:
        sync_service.pump_all_devices(db)
    finally:
        db.close()
    yield


app = FastAPI(
    title="换电站运营管理平台 API",
    description="新能源物流车换电站后台管理（纯后端）。",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/api/health", tags=["系统"])
def health():
    return {"status": "ok", "service": "swap-station-admin"}


app.include_router(auth.router)
app.include_router(stations.router)
app.include_router(vehicles.router)
app.include_router(swaps.router)
app.include_router(sync.router)
app.include_router(dashboard.router)
