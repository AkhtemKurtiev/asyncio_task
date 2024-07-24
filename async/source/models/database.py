"""Модуль для подключения к базе данных postgreql."""

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import async_sessionmaker


from .config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER

DATABASE_URL = (
    f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

BaseModel = declarative_base()

async_engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    expire_on_commit=False
)


async def create_db():
    """."""
    async with async_engine.begin() as conn:
        await conn.run_sync(BaseModel.metadata.create_all)
