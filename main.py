"""
main.py - Space Weather API (FastAPI + Postgres)

Assumptions (matches your pgAdmin screenshot):
- Database: spaceweather
- Schema: public
- Tables:
    - events(id, source, event_type, severity, message, issued_at, raw)
    - observations(id, source, metric, value, unit, observed_at)

You can keep ingest_noaa.py as your ingestor. This API reads from DB, and
also fetches LIVE NOAA 3-day Kp forecast for "geomagnetic storm forecast".
"""

from __future__ import annotations

import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from sqlalchemy import (
    create_engine,
    text,
    Integer,
    String,
    Float,
    DateTime,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


# ============================================================
# Hardcoded DB URL (as requested)
# ============================================================
DATABASE_URL = "postgresql+psycopg://postgres:password@127.0.0.1:5432/spaceweather"


# ============================================================
# NOAA SWPC 3-day Kp forecast product
# (returns JSON array with header row + data rows)
# Example product name: "NOAA Planetary K-index Forecast"
# ============================================================
NOAA_KP_FORECAST_URL = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index-forecast.json"


# ============================================================
# FastAPI init
# ============================================================
app = FastAPI(
    title="Space Weather API",
    version="0.1.0",
    description="FastAPI backend for NOAA ingestion + querying + forecast + annual reports.",
)


# ============================================================
# SQLAlchemy setup
# ============================================================
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


# Match your existing tables exactly
class Event(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    event_type: Mapped[str] = mapped_column(String(20), nullable=False)
    severity: Mapped[str] = mapped_column(String(10), nullable=False)
    message: Mapped[str] = mapped_column(String, nullable=False)
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False)


class Observation(Base):
    __tablename__ = "observations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    metric: Mapped[str] = mapped_column(String(50), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    unit: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


# ============================================================
# Pydantic response models
# ============================================================
class HealthOut(BaseModel):
    ok: bool
    database_url: str
    db_time_utc: str


class EventOut(BaseModel):
    id: int
    source: str
    event_type: str
    severity: str
    message: str
    issued_at: datetime
    raw: dict


class ObservationOut(BaseModel):
    id: int
    source: str
    metric: str
    value: float
    unit: Optional[str] = None
    observed_at: datetime


class ForecastPoint(BaseModel):
    time_tag: datetime
    kp: float
    g_scale: str = Field(..., description="Estimated NOAA geomagnetic storm scale from Kp (G0..G5)")


class ForecastDaySummary(BaseModel):
    date: date
    kp_max: float
    g_scale_max: str
    points: List[ForecastPoint]


class GeomagneticForecastOut(BaseModel):
    source: str
    generated_at_utc: datetime
    days: int
    daily: List[ForecastDaySummary]


class AnnualReportOut(BaseModel):
    year: int
    generated_at_utc: datetime

    # Events summary
    total_events: int
    events_by_severity: Dict[str, int]
    top_event_types: List[Dict[str, Any]]

    # Kp-based storm summary (if you have metric="kp" in observations)
    kp_points: int
    kp_max: Optional[float]
    storm_intervals_kp_ge_5: int  # count of rows where kp>=5
    storm_days_estimated: int     # days where daily max kp>=5

    # Generic metric overview
    metrics_available: List[str]


# ============================================================
# Utilities
# ============================================================
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def kp_to_g_scale(kp: float) -> str:
    """
    NOAA G-scale mapping commonly used:
    Kp 0-4 => G0 (quiet to unsettled)
    Kp 5 => G1
    Kp 6 => G2
    Kp 7 => G3
    Kp 8 => G4
    Kp 9 => G5
    """
    try:
        k = float(kp)
    except Exception:
        return "G0"

    if k >= 9:
        return "G5"
    if k >= 8:
        return "G4"
    if k >= 7:
        return "G3"
    if k >= 6:
        return "G2"
    if k >= 5:
        return "G1"
    return "G0"


def parse_noaa_kp_forecast() -> List[Tuple[datetime, float]]:
    """
    NOAA product is typically:
    [
      ["time_tag","kp","observed","..."],
      ["2026-01-14 00:00:00","3.67","predicted",...],
      ...
    ]
    We only use time_tag + kp.
    """
    r = requests.get(NOAA_KP_FORECAST_URL, timeout=30)
    r.raise_for_status()
    raw = r.json()

    if not isinstance(raw, list) or len(raw) < 2 or not isinstance(raw[0], list):
        raise ValueError("Unexpected NOAA forecast JSON structure.")

    header = raw[0]
    idx = {name: i for i, name in enumerate(header)}
    if "time_tag" not in idx or "kp" not in idx:
        raise ValueError("NOAA forecast missing expected columns time_tag/kp.")

    out: List[Tuple[datetime, float]] = []
    for row in raw[1:]:
        if not isinstance(row, list) or len(row) < len(header):
            continue
        ts_raw = str(row[idx["time_tag"]]).strip()
        kp_raw = str(row[idx["kp"]]).strip()

        # NOAA often uses "YYYY-MM-DD HH:MM:SS"
        try:
            ts = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            # try ISO
            try:
                ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                else:
                    ts = ts.astimezone(timezone.utc)
            except Exception:
                continue

        try:
            kp = float(kp_raw)
        except Exception:
            continue

        out.append((ts, kp))

    # Sort by time
    out.sort(key=lambda x: x[0])
    return out


@dataclass
class _Cache:
    value: Any = None
    expires_at: float = 0.0


_forecast_cache = _Cache()


def get_cached_forecast(ttl_seconds: int = 600) -> List[Tuple[datetime, float]]:
    now = time.time()
    if _forecast_cache.value is not None and now < _forecast_cache.expires_at:
        return _forecast_cache.value

    data = parse_noaa_kp_forecast()
    _forecast_cache.value = data
    _forecast_cache.expires_at = now + ttl_seconds
    return data


def group_forecast_by_day(points: List[Tuple[datetime, float]], days: int) -> List[ForecastDaySummary]:
    """
    Take forecast points and produce daily summaries for next N days (UTC).
    """
    if days < 1:
        days = 1
    if days > 7:
        days = 7  # keep it reasonable

    start_day = utcnow().date()
    end_day = start_day + timedelta(days=days)

    by_day: Dict[date, List[ForecastPoint]] = defaultdict(list)
    for ts, kp in points:
        d = ts.date()
        if start_day <= d < end_day:
            by_day[d].append(ForecastPoint(time_tag=ts, kp=kp, g_scale=kp_to_g_scale(kp)))

    daily: List[ForecastDaySummary] = []
    for i in range(days):
        d = start_day + timedelta(days=i)
        pts = sorted(by_day.get(d, []), key=lambda p: p.time_tag)
        kp_max = max([p.kp for p in pts], default=0.0)
        daily.append(
            ForecastDaySummary(
                date=d,
                kp_max=kp_max,
                g_scale_max=kp_to_g_scale(kp_max),
                points=pts,
            )
        )
    return daily


# ============================================================
# Routes
# ============================================================
@app.get("/health", response_model=HealthOut)
def health():
    try:
        with SessionLocal() as db:
            db_time = db.execute(text("select now() at time zone 'utc'")).scalar_one()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB not reachable: {e}")

    return HealthOut(ok=True, database_url=DATABASE_URL, db_time_utc=str(db_time))


@app.get("/", response_class=HTMLResponse)
def home(days: int = Query(3, ge=1, le=7)):
    """
    Simple browser-friendly page showing the 3-day geomagnetic forecast,
    plus links to JSON endpoints.
    """
    try:
        points = get_cached_forecast()
        daily = group_forecast_by_day(points, days=days)
    except Exception as e:
        return HTMLResponse(f"<h2>Forecast error</h2><pre>{e}</pre>", status_code=500)

    rows = []
    for day in daily:
        rows.append(
            f"<tr><td>{day.date}</td><td>{day.kp_max:.2f}</td><td>{day.g_scale_max}</td>"
            f"<td>{len(day.points)}</td></tr>"
        )

    html = f"""
    <html>
    <head><title>Space Weather API</title></head>
    <body style="font-family: Arial; max-width: 900px; margin: 30px auto;">
      <h1>Space Weather API</h1>
      <p><b>3-day geomagnetic forecast</b> (UTC, based on NOAA Kp forecast)</p>

      <table border="1" cellpadding="8" cellspacing="0">
        <tr><th>Date</th><th>Max Kp</th><th>Storm Scale</th><th>Points</th></tr>
        {''.join(rows)}
      </table>

      <h3>Endpoints</h3>
      <ul>
        <li><a href="/forecast/geomagnetic?days={days}">/forecast/geomagnetic?days={days}</a></li>
        <li><a href="/reports/annual/2025">/reports/annual/2025</a></li>
        <li><a href="/events?limit=20">/events?limit=20</a></li>
        <li><a href="/observations?metric=kp&limit=20">/observations?metric=kp&limit=20</a></li>
        <li><a href="/docs">Swagger UI (/docs)</a></li>
      </ul>

      <p style="color:#555">Tip: Use <code>/docs</code> to try endpoints in-browser.</p>
    </body>
    </html>
    """
    return HTMLResponse(html)


@app.get("/forecast/geomagnetic", response_model=GeomagneticForecastOut)
def geomagnetic_forecast(days: int = Query(3, ge=1, le=7)):
    """
    Live 3-day geomagnetic storm forecast (UTC), derived from NOAA Kp forecast product.
    """
    try:
        points = get_cached_forecast()
        daily = group_forecast_by_day(points, days=days)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"NOAA forecast fetch/parse failed: {e}")

    return GeomagneticForecastOut(
        source="NOAA SWPC (Kp forecast product)",
        generated_at_utc=utcnow(),
        days=days,
        daily=daily,
    )


@app.get("/events", response_model=List[EventOut])
def list_events(
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    severity: Optional[str] = None,
    event_type: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
):
    """
    Query stored events from your DB.
    """
    with SessionLocal() as db:
        q = db.query(Event)

        if start:
            q = q.filter(Event.issued_at >= start)
        if end:
            q = q.filter(Event.issued_at <= end)
        if severity:
            q = q.filter(Event.severity == severity)
        if event_type:
            q = q.filter(Event.event_type == event_type)

        rows = q.order_by(Event.issued_at.desc()).limit(limit).all()

    return [
        EventOut(
            id=r.id,
            source=r.source,
            event_type=r.event_type,
            severity=r.severity,
            message=r.message,
            issued_at=r.issued_at,
            raw=r.raw,
        )
        for r in rows
    ]


@app.get("/observations", response_model=List[ObservationOut])
def list_observations(
    metric: Optional[str] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    limit: int = Query(200, ge=1, le=2000),
):
    """
    Query stored observations from your DB (time-series metrics).
    """
    with SessionLocal() as db:
        q = db.query(Observation)
        if metric:
            q = q.filter(Observation.metric == metric)
        if start:
            q = q.filter(Observation.observed_at >= start)
        if end:
            q = q.filter(Observation.observed_at <= end)

        rows = q.order_by(Observation.observed_at.desc()).limit(limit).all()

    return [
        ObservationOut(
            id=r.id,
            source=r.source,
            metric=r.metric,
            value=r.value,
            unit=r.unit,
            observed_at=r.observed_at,
        )
        for r in rows
    ]


@app.get("/reports/annual/{year}", response_model=AnnualReportOut)
def annual_report(year: int):
    """
    Annual historical report based on what you have ingested into:
    - events
    - observations

    Notes:
    - This report will include a Kp-storm summary if you have observation metric="kp".
    - If your ingestor uses another metric name (e.g., "planetary_kp"), change it below.
    """
    if year < 1900 or year > 2200:
        raise HTTPException(status_code=400, detail="Year out of supported range.")

    start_dt = datetime(year, 1, 1, tzinfo=timezone.utc)
    end_dt = datetime(year + 1, 1, 1, tzinfo=timezone.utc)

    kp_metric_names = ["kp", "planetary_kp", "kp_index"]  # we will try these in order

    with SessionLocal() as db:
        # --- Events summary ---
        events_rows = (
            db.query(Event)
            .filter(Event.issued_at >= start_dt, Event.issued_at < end_dt)
            .all()
        )

        total_events = len(events_rows)
        sev_counts = Counter([e.severity for e in events_rows])

        type_counts = Counter([e.event_type for e in events_rows])
        top_event_types = [
            {"event_type": t, "count": c} for t, c in type_counts.most_common(10)
        ]

        # --- Metrics available (overall in DB) ---
        metrics_available = [
            r[0]
            for r in db.execute(text("select distinct metric from observations order by metric")).all()
        ]

        # --- Kp summary (if available) ---
        chosen_kp_metric = None
        for cand in kp_metric_names:
            if cand in metrics_available:
                chosen_kp_metric = cand
                break

        kp_points = 0
        kp_max = None
        storm_intervals = 0
        storm_days = 0

        if chosen_kp_metric:
            kp_rows = (
                db.query(Observation.observed_at, Observation.value)
                .filter(
                    Observation.metric == chosen_kp_metric,
                    Observation.observed_at >= start_dt,
                    Observation.observed_at < end_dt,
                )
                .order_by(Observation.observed_at.asc())
                .all()
            )

            kp_points = len(kp_rows)
            if kp_points > 0:
                values = [float(v) for _, v in kp_rows]
                kp_max = max(values)
                storm_intervals = sum(1 for v in values if v >= 5.0)

                # estimate storm days: group by date and check daily max >=5
                day_to_max = defaultdict(lambda: -1.0)
                for ts, v in kp_rows:
                    d = ts.astimezone(timezone.utc).date()
                    day_to_max[d] = max(day_to_max[d], float(v))
                storm_days = sum(1 for _, vmax in day_to_max.items() if vmax >= 5.0)

    return AnnualReportOut(
        year=year,
        generated_at_utc=utcnow(),
        total_events=total_events,
        events_by_severity=dict(sev_counts),
        top_event_types=top_event_types,
        kp_points=kp_points,
        kp_max=kp_max,
        storm_intervals_kp_ge_5=storm_intervals,
        storm_days_estimated=storm_days,
        metrics_available=metrics_available,
    )
