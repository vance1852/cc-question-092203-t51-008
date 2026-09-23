"""数据库连接与会话管理。"""
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from .config import DATABASE_URL

# SQLite 需要关闭同线程检查以配合 FastAPI 的依赖注入；
# timeout 放宽写锁等待，容忍终端并发重发时的短暂锁竞争（应用层另按设备串行化）
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False, "timeout": 15},
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    """FastAPI 依赖：提供一个数据库会话，请求结束后关闭。"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
