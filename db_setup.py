"""
db_setup.py
- Creates tables in Postgres (Docker)
- Inserts sample space weather data
- Verifies insert by querying latest rows

Make sure your docker-compose has:
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_DB=spaceweather
and container is running on localhost:5432
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import OperationalError


# ✅ Hardcode for now to avoid env confusion
DATABASE_URL = "postgresql+psycopg://postgres:password@127.0.0.1:5432/spaceweather"

print(">>> USING DATABASE_URL <<<")
print(DATABASE_URL)
print(">>> END DATABASE_URL <<<\n")


Base = declarative_base()


class Observation(Base):
    __tablename__ = "observations"

    id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False)             # e.g., noaa_swpc
    metric = Column(String(50), nullable=False)             # kp, solar_wind_speed, bt
    value = Column(Float, nullable=False)
    unit = Column(String(20), nullable=True)                # index, km/s, nT
    observed_at = Column(DateTime(timezone=True), nullable=False)


class Event(Base):
    __tablename__ = "events"

    id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False)             # e.g., noaa_swpc
    event_type = Column(String(20), nullable=False)         # G, S, R, ALERT
    severity = Column(String(10), nullable=True)            # G1..G5, S1..S5, R1..R5
    message = Column(Text, nullable=False)
    issued_at = Column(DateTime(timezone=True), nullable=False)
    raw = Column(JSONB, nullable=True)


def main() -> None:
    # echo=True prints SQL; helpful while debugging
    engine = create_engine(DATABASE_URL, echo=True)

    try:
        # 1) Create tables
        Base.metadata.create_all(engine)
        print("\n✅ Tables created (or already exist): observations, events\n")

        # 2) Create session
        SessionLocal = sessionmaker(bind=engine)
        db = SessionLocal()

        # 3) Insert sample data
        now = datetime.now(timezone.utc)

        sample_obs = [
            Observation(
                source="noaa_swpc",
                metric="kp",
                value=4.0,
                unit="index",
                observed_at=now,
            ),
            Observation(
                source="noaa_swpc",
                metric="solar_wind_speed",
                value=520.0,
                unit="km/s",
                observed_at=now,
            ),
            Observation(
                source="noaa_swpc",
                metric="bt",
                value=8.1,
                unit="nT",
                observed_at=now,
            ),
        ]

        sample_event = Event(
            source="noaa_swpc",
            event_type="G",
            severity="G1",
            message="Geomagnetic storm conditions observed (sample record).",
            issued_at=now,
            raw={"sample": True, "note": "Replace with real NOAA payload later"},
        )

        db.add_all(sample_obs)
        db.add(sample_event)
        db.commit()
        print("✅ Inserted sample observations + event\n")

        # 4) Verify inserts
        latest_kp: Optional[Observation] = (
            db.query(Observation)
            .filter(Observation.metric == "kp")
            .order_by(Observation.observed_at.desc())
            .first()
        )

        latest_event: Optional[Event] = (
            db.query(Event)
            .order_by(Event.issued_at.desc())
            .first()
        )

        if latest_kp:
            print(f"📌 Latest Kp: {latest_kp.value} ({latest_kp.observed_at})")
        else:
            print("⚠️ No Kp rows found.")

        if latest_event:
            print(f"📌 Latest Event: {latest_event.severity} - {latest_event.message}")
        else:
            print("⚠️ No events found.")

        db.close()

        print("\n🎉 DB setup completed successfully.\n")

    except OperationalError as e:
        print("\n❌ Could not connect to the database.")
        print("Double-check Docker is running and DATABASE_URL is correct.\n")
        raise e


if __name__ == "__main__":
    main()
