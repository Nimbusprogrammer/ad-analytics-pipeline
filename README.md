# Ad Campaign Analytics Pipeline

An ETL pipeline and REST API for ingesting, processing, and analyzing marketing campaign performance data across multiple ad channels.

Built to simulate real-world marketing analytics workflows — similar to systems used in media measurement and marketing mix modeling.

## What it does

- Ingests raw campaign data (CSV or auto-generated)
- Cleans and transforms records using a pipeline layer
- Stores structured data in a SQLite database
- Exposes REST API endpoints for querying campaign analytics
- Computes key marketing metrics: conversion rate, cost per conversion, spend by channel

## Tech Stack

- **Python** — core language
- **FastAPI** — REST API framework
- **SQLite** — relational database layer
- **Pandas** — data transformation and cleaning
- **Uvicorn** — ASGI server

## Project Structure
```
ad-analytics-pipeline/
├── main.py          # FastAPI app, API endpoints
├── pipeline.py      # ETL pipeline, data ingestion and transformation
├── database.py      # Database connection, schema, queries
├── data/            # Drop CSV files here for ingestion
├── requirements.txt
└── README.md
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | API info and available endpoints |
| GET | `/health` | Health check |
| GET | `/campaigns` | All campaign records |
| GET | `/summary` | Aggregated metrics per channel |
| GET | `/summary/best-channel` | Channel with lowest cost per conversion |
| GET | `/summary/worst-channel` | Channel with highest cost per conversion |

## Setup and Run
```bash
# Install dependencies
pip install -r requirements.txt

# Start the server
uvicorn main:app --reload
```

Visit `http://127.0.0.1:8000/docs` for interactive API documentation.

## Sample API Response
```json
{
  "summary": [
    {
      "channel": "YouTube",
      "total_spend": 685138.1,
      "total_impressions": 32483804,
      "total_clicks": 1821605,
      "total_conversions": 152371,
      "conversion_rate_percent": 13.88,
      "cost_per_conversion": 2.61
    }
  ]
}
```

## CSV Ingestion

To load your own data, drop a CSV file in the `data/` folder with these columns:
```
campaign_name, channel, date, spend, impressions, clicks, conversions
```

Then update `main.py` startup to pass the file path to `run_pipeline()`.