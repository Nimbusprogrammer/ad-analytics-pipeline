from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import Optional

from database import (
    init_db, fetch_all_campaigns, fetch_campaign_detail,
    fetch_campaign_trends, fetch_channel_comparison,
    fetch_daily_totals, fetch_anomalies, fetch_kpi_summary,
)
from pipeline import run_pipeline, get_pipeline_status
from models import (
    CampaignOverview, ChannelSummary, TrendPoint, AnomalyRecord,
    DailyTotal, KPISummary, PipelineStatus,
)


@asynccontextmanager
async def lifespan(app):
    init_db()
    run_pipeline(num_campaigns=20, days=90)
    print("Server ready.")
    yield


app = FastAPI(
    title="Ad Campaign Analytics Pipeline",
    description=(
        "Multi-stage ETL pipeline and REST API for marketing campaign "
        "performance analysis. Features normalized database design, "
        "data validation, anomaly detection, and interactive dashboards."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

templates = Jinja2Templates(directory="templates")


# ── Root & Health ──────────────────────────────────────────────

@app.get("/")
def root():
    return {
        "message": "Ad Campaign Analytics Pipeline API v2.0",
        "docs": "/docs",
        "dashboard": "/dashboard",
        "endpoints": {
            "campaigns": {
                "list": "/campaigns",
                "detail": "/campaigns/{id}",
                "trend": "/campaigns/{id}/trend",
            },
            "analytics": {
                "kpi": "/analytics/kpi",
                "channel_comparison": "/analytics/channels",
                "daily_totals": "/analytics/daily?start=YYYY-MM-DD&end=YYYY-MM-DD",
                "anomalies": "/analytics/anomalies",
                "best_channel": "/analytics/best-channel",
                "worst_channel": "/analytics/worst-channel",
            },
            "pipeline": "/pipeline/status",
            "health": "/health",
        },
    }


@app.get("/health")
def health():
    status = get_pipeline_status()
    return {
        "status": "ok",
        "pipeline_status": status["status"],
        "last_pipeline_run": status["last_run"],
    }


# ── Campaigns ─────────────────────────────────────────────────

@app.get("/campaigns", response_model=None)
def list_campaigns():
    rows = fetch_all_campaigns()
    if not rows:
        raise HTTPException(status_code=404, detail="No campaign data found")
    return {"total": len(rows), "campaigns": rows}


@app.get("/campaigns/{campaign_id}", response_model=None)
def get_campaign(campaign_id: int):
    campaign = fetch_campaign_detail(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail=f"Campaign {campaign_id} not found")
    return campaign


@app.get("/campaigns/{campaign_id}/trend", response_model=None)
def get_campaign_trend(campaign_id: int):
    # Verify campaign exists
    campaign = fetch_campaign_detail(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail=f"Campaign {campaign_id} not found")

    trends = fetch_campaign_trends(campaign_id)
    return {
        "campaign_id": campaign_id,
        "campaign_name": campaign["campaign_name"],
        "channel": campaign["channel"],
        "data_points": len(trends),
        "trend": trends,
    }


# ── Analytics ─────────────────────────────────────────────────

@app.get("/analytics/kpi", response_model=None)
def get_kpi():
    kpi = fetch_kpi_summary()
    if not kpi:
        raise HTTPException(status_code=404, detail="No data available for KPI computation")
    return kpi


@app.get("/analytics/channels", response_model=None)
def get_channel_comparison():
    rows = fetch_channel_comparison()
    if not rows:
        raise HTTPException(status_code=404, detail="No channel data found")
    return {"total_channels": len(rows), "channels": rows}


@app.get("/analytics/daily", response_model=None)
def get_daily_totals(
    start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
):
    rows = fetch_daily_totals(start, end)
    return {"data_points": len(rows), "daily": rows}


@app.get("/analytics/anomalies", response_model=None)
def get_anomalies(
    threshold: float = Query(2.0, description="Standard deviation threshold for anomaly detection"),
):
    rows = fetch_anomalies(threshold)
    return {
        "threshold": threshold,
        "anomalies_found": len(rows),
        "anomalies": rows,
    }


@app.get("/analytics/best-channel")
def best_channel():
    rows = fetch_channel_comparison()
    if not rows:
        raise HTTPException(status_code=404, detail="No data found")
    best = max(rows, key=lambda x: x["roi_percent"])
    return {
        "best_channel": best["channel"],
        "roi_percent": best["roi_percent"],
        "total_revenue": best["total_revenue"],
        "total_spend": best["total_spend"],
        "conversion_rate": best["conversion_rate"],
    }


@app.get("/analytics/worst-channel")
def worst_channel():
    rows = fetch_channel_comparison()
    if not rows:
        raise HTTPException(status_code=404, detail="No data found")
    worst = min(rows, key=lambda x: x["roi_percent"])
    return {
        "worst_channel": worst["channel"],
        "roi_percent": worst["roi_percent"],
        "total_revenue": worst["total_revenue"],
        "total_spend": worst["total_spend"],
        "conversion_rate": worst["conversion_rate"],
    }


# ── Pipeline ──────────────────────────────────────────────────

@app.get("/pipeline/status", response_model=None)
def pipeline_status():
    return get_pipeline_status()


@app.post("/pipeline/run")
def trigger_pipeline(
    campaigns: int = Query(20, description="Number of campaigns to generate"),
    days: int = Query(90, description="Number of days of data"),
):
    result = run_pipeline(num_campaigns=campaigns, days=days)
    return {"message": "Pipeline executed successfully", "result": result}


# ── Dashboard ─────────────────────────────────────────────────

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse(request, "dashboard.html")
