from __future__ import annotations

import argparse
import time
from datetime import datetime
from typing import Any, List, Optional, Tuple

import requests
from sqlalchemy import (
    DateTime,
    Float,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


# ============================================================
# HARD-CODED DATABASE URL (as requested)
# ============================================================
DATABASE_URL = "postgresql+psycopg://postgres:password@127.0.0.1:5432/spaceweather"


# ============================================================
# NOAA SWPC endpoints
# ============================================================
ALERTS_URL = "https://services.swpc.noaa.gov/products/alerts.json"
RTSW_WIND_URL = "https://services.swpc.noaa.gov/json/rtsw/rtsw_wind_1m.json"
RTSW_MAG_URL = "https://services.swpc.noaa.gov/json/rtsw/rtsw_mag_1m.json"


# ============================================================
# Helpers
# ============================================================
def fetch_json(url: str, timeout: int = 30) -> Any:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def parse_datetime_loose(s: str) -> datetime:
    """
    NOAA feeds vary in datetime formatting.
    We'll try multiple formats.
    """
    s = str(s).strip()

    fmts = [
        "%Y-%m-%d %H:%M:%S.%f",  # alerts.json often uses microseconds
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for f in fmts:
        try:
            return datetime.strptime(s, f)
        except ValueError:
            continue

    # If timezone appears like +00:00, trim and retry
    if s.endswith("+00:00"):
        s2 = s.replace("+00:00", "")
        for f in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
            try:
                return datetime.strptime(s2, f)
            except ValueError:
                pass

    raise ValueError(f"Unrecognized datetime format: {s!r}")


def safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "" or s.lower() in {"null", "none", "nan"}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def normalize_rtsw_rows(raw: Any) -> Tuple[List[str], List[List[Any]]]:
    """
    RTSW JSON typically looks like:
      [
        ["time_tag","speed","density","temperature", ...],
        ["2026-..", "420.3", "6.2", "123456", ...],
        ...
      ]
    Returns (header, data_rows)
    """
    if not isinstance(raw, list) or len(raw) < 2:
        return [], []

    header = raw[0]
    rows = raw[1:]

    if not isinstance(header, list):
        return [], []

    clean_rows: List[List[Any]] = []
    for r in rows:
        if isinstance(r, list) and len(r) == len(header):
            clean_rows.append(r)

    return header, clean_rows


# ============================================================
# SQLAlchemy Models
# ============================================================
class Base(DeclarativeBase):
    pass


class SwpcAlert(Base):
    __tablename__ = "swpc_alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    product_id: Mapped[str] = mapped_column(String(16), nullable=False)
    issue_datetime: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    message: Mapped[str] = mapped_column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint("product_id", "issue_datetime", name="uq_alert_product_issue"),
    )


class RtswWind1m(Base):
    __tablename__ = "rtsw_wind_1m"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    time_tag: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    speed: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    density: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    temperature: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint("time_tag", name="uq_rtsw_wind_time_tag"),
    )


class RtswMag1m(Base):
    __tablename__ = "rtsw_mag_1m"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    time_tag: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    bt: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    bz: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    by: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint("time_tag", name="uq_rtsw_mag_time_tag"),
    )


# ============================================================
# DB
# ============================================================
def make_engine():
    print(">>> USING DATABASE_URL <<<")
    print(DATABASE_URL)
    print(">>> END DATABASE_URL <<<\n")
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def init_db(engine) -> None:
    Base.metadata.create_all(engine)
    print("✅ DB initialized (tables created if missing).")


def smoke_test(session: Session) -> None:
    session.execute(text("select 1"))
    print("✅ DB connection OK.")


# ============================================================
# Ingestors
# ============================================================
def ingest_alerts(session: Session) -> int:
    data = fetch_json(ALERTS_URL)

    if not isinstance(data, list):
        print("⚠️ alerts.json returned unexpected JSON shape.")
        return 0

    inserted = 0
    for item in data:
        if not isinstance(item, dict):
            continue

        product_id = str(item.get("product_id", "")).strip()
        issue_dt_raw = str(item.get("issue_datetime", "")).strip()
        message = str(item.get("message", "")).strip()

        if not product_id or not issue_dt_raw or not message:
            continue

        try:
            issue_dt = parse_datetime_loose(issue_dt_raw)
        except Exception:
            continue

        exists = session.query(SwpcAlert.id).filter(
            SwpcAlert.product_id == product_id,
            SwpcAlert.issue_datetime == issue_dt,
        ).first()

        if exists:
            continue

        session.add(
            SwpcAlert(
                product_id=product_id,
                issue_datetime=issue_dt,
                message=message,
            )
        )
        inserted += 1

    session.commit()
    print(f"✅ Alerts ingested. New rows: {inserted}")
    return inserted


def ingest_rtsw_wind(session: Session) -> int:
    raw = fetch_json(RTSW_WIND_URL)
    header, rows = normalize_rtsw_rows(raw)
    if not header:
        print("⚠️ rtsw_wind_1m.json returned unexpected JSON shape.")
        return 0

    idx = {name: i for i, name in enumerate(header)}
    if "time_tag" not in idx:
        print("⚠️ rtsw_wind missing time_tag column.")
        return 0

    inserted = 0
    for r in rows:
        try:
            t = parse_datetime_loose(r[idx["time_tag"]])
        except Exception:
            continue

        speed = safe_float(r[idx["speed"]]) if "speed" in idx else None
        density = safe_float(r[idx["density"]]) if "density" in idx else None
        temperature = safe_float(r[idx["temperature"]]) if "temperature" in idx else None

        exists = session.query(RtswWind1m.id).filter(RtswWind1m.time_tag == t).first()
        if exists:
            continue

        session.add(
            RtswWind1m(
                time_tag=t,
                speed=speed,
                density=density,
                temperature=temperature,
            )
        )
        inserted += 1

    session.commit()
    print(f"✅ RTSW Wind (1m) ingested. New rows: {inserted}")
    return inserted


def ingest_rtsw_mag(session: Session) -> int:
    raw = fetch_json(RTSW_MAG_URL)
    header, rows = normalize_rtsw_rows(raw)
    if not header:
        print("⚠️ rtsw_mag_1m.json returned unexpected JSON shape.")
        return 0

    idx = {name: i for i, name in enumerate(header)}
    if "time_tag" not in idx:
        print("⚠️ rtsw_mag missing time_tag column.")
        return 0

    inserted = 0
    for r in rows:
        try:
            t = parse_datetime_loose(r[idx["time_tag"]])
        except Exception:
            continue

        bt = safe_float(r[idx["bt"]]) if "bt" in idx else None
        bz = safe_float(r[idx["bz"]]) if "bz" in idx else None
        by = safe_float(r[idx["by"]]) if "by" in idx else None

        exists = session.query(RtswMag1m.id).filter(RtswMag1m.time_tag == t).first()
        if exists:
            continue

        session.add(RtswMag1m(time_tag=t, bt=bt, bz=bz, by=by))
        inserted += 1

    session.commit()
    print(f"✅ RTSW Mag (1m) ingested. New rows: {inserted}")
    return inserted


# ============================================================
# Runner
# ============================================================
def run_once(engine) -> None:
    with Session(engine) as session:
        smoke_test(session)
        ingest_alerts(session)
        ingest_rtsw_wind(session)
        ingest_rtsw_mag(session)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--init-db", action="store_true", help="Create tables")
    parser.add_argument("--once", action="store_true", help="Run one ingestion cycle")
    parser.add_argument("--loop", type=int, default=0, help="Loop every N seconds (e.g. --loop 60)")
    args = parser.parse_args()

    engine = make_engine()

    if args.init_db:
        init_db(engine)

    if args.once:
        run_once(engine)

    if args.loop and args.loop > 0:
        print(f"🔁 Looping every {args.loop} seconds. Ctrl+C to stop.")
        while True:
            try:
                run_once(engine)
            except Exception as e:
                print("❌ Ingestion failed:", repr(e))
            time.sleep(args.loop)

    if not (args.init_db or args.once or (args.loop and args.loop > 0)):
        parser.print_help()


if __name__ == "__main__":
    main()
