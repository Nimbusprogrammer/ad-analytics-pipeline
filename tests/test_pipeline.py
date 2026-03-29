import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipeline import generate_sample_data, validate_records, transform_records
from pipeline_config import CHANNELS


def test_generate_produces_correct_count():
    records, report = generate_sample_data(num_campaigns=5, days=30)
    assert len(records) > 0
    assert report["campaigns"] == 5
    assert report["stage"] == "generation"


def test_generate_all_channels_valid():
    records, _ = generate_sample_data(num_campaigns=10, days=30)
    valid_channels = set(CHANNELS.keys())
    for rec in records:
        assert rec["channel"] in valid_channels


def test_generate_records_have_required_fields():
    records, _ = generate_sample_data(num_campaigns=3, days=10)
    required = ["campaign_name", "channel", "date", "spend", "impressions", "clicks", "conversions", "revenue"]
    for rec in records:
        for field in required:
            assert field in rec, f"Missing field: {field}"


def test_validate_accepts_good_records():
    good_records = [{
        "campaign_name": "Test", "channel": "Google Ads", "channel_category": "paid",
        "start_date": "2024-01-01", "end_date": "2024-01-31", "budget": 1000,
        "date": "2024-01-15", "spend": 100, "impressions": 5000,
        "clicks": 200, "conversions": 10, "revenue": 500,
    }]
    valid, rejected, report = validate_records(good_records)
    assert len(valid) == 1
    assert len(rejected) == 0


def test_validate_rejects_negative_spend():
    bad_records = [{
        "campaign_name": "Test", "channel": "Google Ads", "channel_category": "paid",
        "start_date": "2024-01-01", "end_date": "2024-01-31", "budget": 1000,
        "date": "2024-01-15", "spend": -50, "impressions": 5000,
        "clicks": 200, "conversions": 10, "revenue": 500,
    }]
    valid, rejected, report = validate_records(bad_records)
    assert len(valid) == 0
    assert len(rejected) == 1
    assert "negative_spend" in report["rejection_reasons"]


def test_validate_rejects_clicks_exceed_impressions():
    bad_records = [{
        "campaign_name": "Test", "channel": "Facebook", "channel_category": "social",
        "start_date": "2024-01-01", "end_date": "2024-01-31", "budget": 1000,
        "date": "2024-01-15", "spend": 100, "impressions": 100,
        "clicks": 500, "conversions": 10, "revenue": 200,
    }]
    valid, rejected, report = validate_records(bad_records)
    assert len(rejected) == 1
    assert "clicks_exceed_impressions" in report["rejection_reasons"]


def test_validate_rejects_excessive_spend():
    bad_records = [{
        "campaign_name": "Test", "channel": "YouTube", "channel_category": "paid",
        "start_date": "2024-01-01", "end_date": "2024-01-31", "budget": 100000,
        "date": "2024-01-15", "spend": 999999, "impressions": 5000,
        "clicks": 200, "conversions": 10, "revenue": 500,
    }]
    valid, rejected, report = validate_records(bad_records)
    assert len(rejected) == 1
    assert "excessive_spend" in report["rejection_reasons"]


def test_transform_computes_derived_metrics():
    records = [{
        "campaign_name": "Test", "channel": "  google ads  ", "channel_category": "paid",
        "start_date": "2024-01-01", "end_date": "2024-01-31", "budget": 1000,
        "date": "2024-01-15", "spend": 100, "impressions": 10000,
        "clicks": 500, "conversions": 25, "revenue": 750,
    }]
    transformed, report = transform_records(records)
    rec = transformed[0]

    assert rec["channel"] == "Google Ads"  # normalized
    assert rec["ctr"] == 5.0              # 500/10000 * 100
    assert rec["cpc"] == 0.2              # 100/500
    assert rec["conversion_rate"] == 5.0  # 25/500 * 100
    assert rec["roas"] == 7.5             # 750/100


def test_transform_handles_zero_division():
    records = [{
        "campaign_name": "Test", "channel": "Email", "channel_category": "organic",
        "start_date": "2024-01-01", "end_date": "2024-01-31", "budget": 0,
        "date": "2024-01-15", "spend": 0, "impressions": 0,
        "clicks": 0, "conversions": 0, "revenue": 0,
    }]
    transformed, _ = transform_records(records)
    rec = transformed[0]

    assert rec["ctr"] == 0
    assert rec["cpc"] == 0
    assert rec["conversion_rate"] == 0
    assert rec["roas"] == 0


def test_validation_report_structure():
    records, _ = generate_sample_data(num_campaigns=5, days=10)
    _, _, report = validate_records(records)
    assert "total_input" in report
    assert "valid" in report
    assert "rejected" in report
    assert "rejection_reasons" in report
    assert report["total_input"] == report["valid"] + report["rejected"]
