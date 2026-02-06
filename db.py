import os
from datetime import datetime
from typing import Generator

from sqlalchemy import DateTime, Integer, String, func
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
from sqlalchemy import create_engine


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


_ENGINE: Engine | None = None
_SESSION_FACTORY: sessionmaker | None = None


def get_engine() -> Engine:
    """
    Create (or return) a SQLAlchemy engine targeting AWS RDS.

    Requires:
      - DB_URL: SQLAlchemy URL pointing to Postgres via psycopg3, e.g.:
        - postgresql+psycopg://user:password@host:5432/dbname
    """
    global _ENGINE
    if _ENGINE is None:
        db_url = _require_env("DB_URL")
        _ENGINE = create_engine(
            db_url,
            pool_pre_ping=True,
            future=True,
        )
    return _ENGINE


class Base(DeclarativeBase):
    pass


class Image(Base):
    __tablename__ = "images"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    extension: Mapped[str] = mapped_column(String(32), nullable=False)
    s3_key: Mapped[str] = mapped_column(String(1024), nullable=False)

    last_updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


def create_session_factory(engine: Engine) -> sessionmaker:
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def get_session_factory() -> sessionmaker:
    global _SESSION_FACTORY
    if _SESSION_FACTORY is None:
        _SESSION_FACTORY = create_session_factory(get_engine())
    return _SESSION_FACTORY


def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency that yields a SQLAlchemy Session.
    """
    factory = get_session_factory()
    db: Session = factory()
    try:
        yield db
    finally:
        db.close()


def init_db(engine: Engine) -> None:
    """
    Create tables if they don't exist.

    For a small demo-style service this is a pragmatic replacement for migrations.
    """
    Base.metadata.create_all(bind=engine)

