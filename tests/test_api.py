import sys
import os

# Set test database before importing anything
os.environ["DATABASE_PATH"] = os.path.join(os.path.dirname(__file__), "..", "test_analytics.db")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from fastapi.testclient import TestClient
from main import app
from database import init_db
from pipeline import run_pipeline

# Ensure database is populated before tests
init_db()
run_pipeline(num_campaigns=5, days=30)

client = TestClient(app)


@pytest.fixture(scope="session", autouse=True)
def cleanup():
    yield
    db_path = os.environ.get("DATABASE_PATH", "test_analytics.db")
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except PermissionError:
            pass


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "pipeline_status" in data


def test_root_endpoint():
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "endpoints" in data
    assert "dashboard" in data


def test_campaigns_list():
    response = client.get("/campaigns")
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert "campaigns" in data
    assert data["total"] > 0
    campaign = data["campaigns"][0]
    assert "campaign_name" in campaign
    assert "channel" in campaign
    assert "total_spend" in campaign


def test_campaign_detail_valid():
    response = client.get("/campaigns/1")
    assert response.status_code == 200
    data = response.json()
    assert "campaign_name" in data
    assert "daily_metrics" in data
    assert len(data["daily_metrics"]) > 0


def test_campaign_detail_not_found():
    response = client.get("/campaigns/99999")
    assert response.status_code == 404


def test_campaign_trend():
    response = client.get("/campaigns/1/trend")
    assert response.status_code == 200
    data = response.json()
    assert "trend" in data
    assert "campaign_name" in data
    assert data["data_points"] > 0


def test_campaign_trend_not_found():
    response = client.get("/campaigns/99999/trend")
    assert response.status_code == 404


def test_kpi_endpoint():
    response = client.get("/analytics/kpi")
    assert response.status_code == 200
    data = response.json()
    assert "total_campaigns" in data
    assert "total_spend" in data
    assert "total_revenue" in data
    assert "overall_roi" in data
    assert data["total_campaigns"] > 0


def test_channel_comparison():
    response = client.get("/analytics/channels")
    assert response.status_code == 200
    data = response.json()
    assert "channels" in data
    assert len(data["channels"]) > 0
    ch = data["channels"][0]
    assert "roi_percent" in ch
    assert "conversion_rate" in ch
    assert "cost_per_conversion" in ch


def test_daily_totals_no_filter():
    response = client.get("/analytics/daily")
    assert response.status_code == 200
    data = response.json()
    assert "daily" in data
    assert len(data["daily"]) > 0


def test_daily_totals_with_date_filter():
    response = client.get("/analytics/daily?start=2024-07-01&end=2024-08-01")
    assert response.status_code == 200
    data = response.json()
    assert "daily" in data
    for day in data["daily"]:
        assert day["date"] >= "2024-07-01"
        assert day["date"] <= "2024-08-01"


def test_anomalies_endpoint():
    response = client.get("/analytics/anomalies")
    assert response.status_code == 200
    data = response.json()
    assert "anomalies" in data
    assert "threshold" in data
    assert data["threshold"] == 2.0


def test_anomalies_custom_threshold():
    response = client.get("/analytics/anomalies?threshold=1.0")
    assert response.status_code == 200
    data = response.json()
    assert data["threshold"] == 1.0


def test_best_channel():
    response = client.get("/analytics/best-channel")
    assert response.status_code == 200
    data = response.json()
    assert "best_channel" in data
    assert "roi_percent" in data


def test_worst_channel():
    response = client.get("/analytics/worst-channel")
    assert response.status_code == 200
    data = response.json()
    assert "worst_channel" in data
    assert "roi_percent" in data


def test_pipeline_status():
    response = client.get("/pipeline/status")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "completed"
    assert data["records_loaded"] > 0
    assert "stages" in data
    assert "generation" in data["stages"]
    assert "validation" in data["stages"]
    assert "loading" in data["stages"]


def test_dashboard_returns_html():
    response = client.get("/dashboard")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Ad Campaign Analytics" in response.text
