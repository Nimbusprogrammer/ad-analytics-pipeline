# Ad Campaign Analytics Pipeline

A production-style **multi-stage ETL pipeline** and **REST API** for ingesting, validating, transforming, and analyzing marketing campaign performance data across multiple advertising channels.

Features a **normalized relational database**, **statistical anomaly detection**, and an **interactive analytics dashboard** with real-time KPI visualization.

## Architecture

```
                        ┌─────────────────────────────────────────┐
                        │          ETL Pipeline (4 Stages)        │
                        │                                         │
  CSV / Generated  ───► │  Generate ► Validate ► Transform ► Load │
       Data             │     │          │           │         │  │
                        │     ▼          ▼           ▼         ▼  │
                        │  Records   Rejected    Derived    SQLite│
                        │  Created   Flagged     Metrics    Insert│
                        └────────────────────────────────────┬────┘
                                                             │
                        ┌────────────────────────────────────┘
                        ▼
              ┌──────────────────┐        ┌──────────────────────┐
              │   SQLite Database│        │   REST API (FastAPI) │
              │                  │◄──────►│                      │
              │  channels        │        │  14 endpoints        │
              │  campaigns       │        │  Pydantic models     │
              │  daily_metrics   │        │  Query parameters    │
              └──────────────────┘        └──────────┬───────────┘
                                                     │
                                          ┌──────────┘
                                          ▼
                                ┌───────────────────┐
                                │    Dashboard      │
                                │                   │
                                │  KPI Cards        │
                                │  6 Charts         │
                                │  Anomaly Alerts   │
                                │  Campaign Table   │
                                └───────────────────┘
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11 |
| API Framework | FastAPI |
| Database | SQLite (normalized, 3 tables with FKs) |
| Data Processing | Pandas |
| Templating | Jinja2 |
| Visualization | Chart.js |
| Testing | pytest + FastAPI TestClient |
| Containerization | Docker |

## Database Schema

Three normalized tables with foreign key relationships:

```sql
channels (id, name, category)
    │
    └──► campaigns (id, campaign_name, channel_id FK, start_date, end_date, budget, status)
              │
              └──► daily_metrics (id, campaign_id FK, date, spend, impressions, clicks,
                                  conversions, revenue)
```

**Key queries:** JOINs across all 3 tables, GROUP BY aggregations, date-range filtering, and standard-deviation-based anomaly detection using CTEs and subqueries.

## ETL Pipeline

The pipeline runs in 4 stages with logging and timing at each stage:

| Stage | Description |
|-------|-------------|
| **1. Generation** | Creates 1000+ daily metric records across 20 campaigns and 5 channels with realistic patterns (weekday/weekend variation, channel-specific conversion rates, deliberate anomaly injection) |
| **2. Validation** | Checks for negative values, impossible ratios (clicks > impressions), excessive spend. Returns valid records, rejected records, and a validation report with rejection reasons |
| **3. Transformation** | Normalizes channel names, computes derived metrics (CTR, CPC, conversion rate, ROAS) |
| **4. Loading** | Inserts channels, campaigns, and daily metrics into normalized tables with foreign key relationships |

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | API info and endpoint map |
| GET | `/health` | Health check with pipeline status |
| GET | `/campaigns` | All campaigns with aggregated metrics |
| GET | `/campaigns/{id}` | Single campaign detail with daily metrics |
| GET | `/campaigns/{id}/trend` | Time-series data for a campaign |
| GET | `/analytics/kpi` | Overall KPI summary (spend, revenue, ROI, conversion rate) |
| GET | `/analytics/channels` | Channel comparison with ROI, CTR, cost per conversion |
| GET | `/analytics/daily` | Daily aggregates with optional `?start=` and `?end=` date filters |
| GET | `/analytics/anomalies` | Anomaly detection (configurable `?threshold=` for std dev) |
| GET | `/analytics/best-channel` | Highest ROI channel |
| GET | `/analytics/worst-channel` | Lowest ROI channel |
| GET | `/pipeline/status` | Pipeline run status with per-stage timing |
| POST | `/pipeline/run` | Trigger pipeline re-run with configurable parameters |
| GET | `/dashboard` | Interactive analytics dashboard |

## Dashboard

Interactive single-page dashboard with:
- **KPI Cards** — Total Spend, Revenue, ROI, Campaigns, Conversions, Clicks
- **Revenue vs Spend by Channel** — grouped bar chart
- **ROI by Channel** — horizontal bar chart with positive/negative coloring
- **Daily Spend & Revenue Trend** — line chart with area fill
- **Conversion Rate by Channel** — bar chart
- **Spend Distribution** — doughnut chart
- **Anomaly Alerts** — flagged data points with deviation details
- **Campaign Table** — sortable by any column

## Setup and Run

```bash
# Install dependencies
pip install -r requirements.txt

# Start the server
uvicorn main:app --reload

# Or using Docker
docker-compose up --build
```

Visit:
- `http://127.0.0.1:8000/dashboard` — Interactive dashboard
- `http://127.0.0.1:8000/docs` — Swagger API documentation

## Running Tests

```bash
pytest tests/ -v
```

## Project Structure

```
ad-analytics-pipeline/
├── main.py              # FastAPI app, 14 API endpoints, dashboard route
├── database.py          # Normalized schema (3 tables), complex SQL queries
├── pipeline.py          # 4-stage ETL: generate, validate, transform, load
├── pipeline_config.py   # Channel definitions, validation rules, thresholds
├── models.py            # Pydantic response models
├── templates/
│   └── dashboard.html   # Chart.js interactive dashboard
├── tests/
│   ├── test_pipeline.py # Pipeline unit tests (generation, validation, transformation)
│   └── test_api.py      # API integration tests (all endpoints)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```
