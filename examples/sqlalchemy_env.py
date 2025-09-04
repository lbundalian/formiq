# examples/sqlalchemy_env.py
from __future__ import annotations
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

def build_env(db_url: str = ""):
    """Return a generic env. If db_url provided, attach SQLAlchemy reflection."""
    env = {}
    if db_url:
        engine = create_engine(db_url, pool_pre_ping=True, future=True)
        Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
        md = MetaData()
        md.reflect(bind=engine)
        env.update({"session_factory": Session, "metadata": md, "tables": md.tables})
    return env
