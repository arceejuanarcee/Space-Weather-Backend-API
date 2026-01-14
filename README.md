# Space Weather Backend API

A backend system that ingests real-time space weather data from NOAA, stores it in PostgreSQL, and exposes REST APIs for geomagnetic storm forecasting and historical analysis.

This project demonstrates backend/API engineering principles using real scientific data and a production-style architecture.

---

## Features

- Live NOAA Data Ingestion  
  Ingests space weather alerts and time-series observations from NOAA SWPC. Raw payloads are stored for traceability and future reprocessing.

- Geomagnetic Storm Forecast  
  Provides a 3-day geomagnetic storm forecast derived from NOAA Kp-index predictions and mapped to NOAA G-scale levels (G0–G5).

- Historical Reporting  
  Generates annual space weather reports summarizing storm activity, severity, and peak geomagnetic conditions.

- Production-Style Backend  
  FastAPI REST endpoints, PostgreSQL (Dockerized), SQLAlchemy ORM, timezone-aware timestamps, and JSONB storage.

---

## System Architecture

NOAA SWPC APIs  
→ ingest_noaa.py  
→ PostgreSQL (Docker)  
→ FastAPI (main.py)  
→ Frontend / Client (future)

---

## Database Schema

### events
Stores discrete space weather alerts.

- id (integer, primary key)
- source (varchar)
- event_type (varchar)
- severity (varchar)
- message (text)
- issued_at (timestamptz)
- raw (jsonb)

### observations
Stores time-series space weather metrics.

- id (integer, primary key)
- source (varchar)
- metric (varchar)
- value (double precision)
- unit (varchar)
- observed_at (timestamptz)

---

## API Endpoints

- GET /health  
- GET /forecast/geomagnetic?days=3  
- GET /reports/annual/{year}  
- GET /events  
- GET /observations  

Interactive documentation is available at /docs when the server is running.

---

## Tech Stack

- Backend: FastAPI (Python)
- Database: PostgreSQL
- ORM: SQLAlchemy
- Containerization: Docker / Docker Compose
- External Data Source: NOAA SWPC APIs

---

## Running the Project

Start PostgreSQL:

docker compose up -d

Initialize the database:

python db_setup.py

Ingest NOAA data:

python ingest_noaa.py --once

Run the API server:

uvicorn main:app --reload

API documentation:

http://127.0.0.1:8000/docs

---

## Project Rationale

This project focuses on backend engineering rather than frontend visualization. It emphasizes real-world data ingestion, database modeling, and REST API design. The system is designed to be extensible with scheduling, authentication, and frontend clients.

---

## License

MIT
