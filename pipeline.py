import random
import time
import logging
from datetime import datetime, timedelta

from pipeline_config import (
    CHANNELS, CAMPAIGNS, VALIDATION_RULES,
    ANOMALY_INJECTION_RATE, REVENUE_PER_CONVERSION_RANGE,
)
from database import (
    insert_channel, insert_campaign, insert_daily_metrics, reset_db, init_db,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("pipeline")

pipeline_status = {
    "last_run": None,
    "status": "idle",
    "records_generated": 0,
    "records_validated": 0,
    "records_rejected": 0,
    "records_loaded": 0,
    "duration_seconds": 0,
    "stages": {},
}


# ---------------------------------------------------------------------------
# Stage 1: Data Generation
# ---------------------------------------------------------------------------

def generate_sample_data(num_campaigns=20, days=90):
    """Generate realistic advertising data with weekday/weekend patterns and anomalies."""
    stage_start = time.time()
    logger.info(f"Stage 1 - Generating data: {num_campaigns} campaigns over {days} days")

    start_date = datetime(2024, 7, 1)
    generated = []

    channel_names = list(CHANNELS.keys())
    campaign_names = list(CAMPAIGNS.keys())

    campaign_defs = []
    for i in range(num_campaigns):
        cname = campaign_names[i % len(campaign_names)]
        channel = channel_names[i % len(channel_names)]
        cfg = CAMPAIGNS[cname]
        ch_cfg = CHANNELS[channel]

        duration = random.randint(*cfg["duration_days"])
        budget = round(random.uniform(*cfg["budget_range"]), 2)
        c_start = start_date + timedelta(days=random.randint(0, max(0, days - duration)))
        c_end = c_start + timedelta(days=duration)

        campaign_defs.append({
            "campaign_name": f"{cname} #{i+1}",
            "channel": channel,
            "channel_config": ch_cfg,
            "start_date": c_start,
            "end_date": c_end,
            "budget": budget,
            "daily_budget": budget / duration,
        })

    for cdef in campaign_defs:
        current = cdef["start_date"]
        ch_cfg = cdef["channel_config"]
        conv_lo, conv_hi = ch_cfg["conv_rate_range"]
        rev_lo, rev_hi = REVENUE_PER_CONVERSION_RANGE

        while current <= cdef["end_date"]:
            is_weekend = current.weekday() >= 5
            weekend_factor = 0.6 if is_weekend else 1.0

            daily_spend = cdef["daily_budget"] * weekend_factor * random.uniform(0.7, 1.3)
            daily_spend = round(max(0, daily_spend), 2)

            impressions = int(daily_spend * random.uniform(80, 300))
            ctr = random.uniform(0.01, 0.08) * weekend_factor
            clicks = max(0, int(impressions * ctr))
            conv_rate = random.uniform(conv_lo, conv_hi)
            conversions = max(0, int(clicks * conv_rate))
            rev_per_conv = random.uniform(rev_lo, rev_hi)
            revenue = round(conversions * rev_per_conv, 2)

            # Inject anomalies
            if random.random() < ANOMALY_INJECTION_RATE:
                anomaly_type = random.choice(["spike", "zero_conv", "high_cpc"])
                if anomaly_type == "spike":
                    daily_spend = round(daily_spend * random.uniform(3, 8), 2)
                    impressions = int(impressions * random.uniform(0.5, 1.0))
                elif anomaly_type == "zero_conv":
                    conversions = 0
                    revenue = 0
                elif anomaly_type == "high_cpc":
                    clicks = max(1, clicks // 5)
                    conversions = max(0, conversions // 5)

            generated.append({
                "campaign_name": cdef["campaign_name"],
                "channel": cdef["channel"],
                "channel_category": ch_cfg["category"],
                "start_date": cdef["start_date"].strftime("%Y-%m-%d"),
                "end_date": cdef["end_date"].strftime("%Y-%m-%d"),
                "budget": cdef["budget"],
                "date": current.strftime("%Y-%m-%d"),
                "spend": daily_spend,
                "impressions": impressions,
                "clicks": clicks,
                "conversions": conversions,
                "revenue": revenue,
            })

            current += timedelta(days=1)

    elapsed = round(time.time() - stage_start, 3)
    logger.info(f"Stage 1 complete: {len(generated)} daily records generated in {elapsed}s")
    return generated, {"stage": "generation", "records": len(generated), "campaigns": num_campaigns, "duration_s": elapsed}


# ---------------------------------------------------------------------------
# Stage 2: Validation
# ---------------------------------------------------------------------------

def validate_records(records):
    """Validate records against business rules. Returns (valid, rejected, report)."""
    stage_start = time.time()
    logger.info(f"Stage 2 - Validating {len(records)} records")

    valid = []
    rejected = []
    rejection_reasons = {}

    for rec in records:
        issues = []

        if rec["spend"] < VALIDATION_RULES["min_spend"]:
            issues.append("negative_spend")
        if rec["spend"] > VALIDATION_RULES["max_spend_per_day"]:
            issues.append("excessive_spend")
        if rec["impressions"] > VALIDATION_RULES["max_impressions_per_day"]:
            issues.append("excessive_impressions")
        if rec["impressions"] < 0:
            issues.append("negative_impressions")
        if rec["clicks"] < 0:
            issues.append("negative_clicks")
        if rec["clicks"] > rec["impressions"] and rec["impressions"] > 0:
            issues.append("clicks_exceed_impressions")
        if rec["conversions"] < 0:
            issues.append("negative_conversions")
        if rec["conversions"] > rec["clicks"] and rec["clicks"] > 0:
            issues.append("conversions_exceed_clicks")

        if issues:
            rec["rejection_reasons"] = issues
            rejected.append(rec)
            for reason in issues:
                rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
        else:
            valid.append(rec)

    elapsed = round(time.time() - stage_start, 3)
    report = {
        "stage": "validation",
        "total_input": len(records),
        "valid": len(valid),
        "rejected": len(rejected),
        "rejection_reasons": rejection_reasons,
        "duration_s": elapsed,
    }
    logger.info(f"Stage 2 complete: {len(valid)} valid, {len(rejected)} rejected in {elapsed}s")
    return valid, rejected, report


# ---------------------------------------------------------------------------
# Stage 3: Transformation
# ---------------------------------------------------------------------------

def transform_records(records):
    """Normalize and enrich records with derived metrics."""
    stage_start = time.time()
    logger.info(f"Stage 3 - Transforming {len(records)} records")

    for rec in records:
        # Normalize channel names
        rec["channel"] = rec["channel"].strip().title()

        # Compute derived metrics
        rec["ctr"] = round(rec["clicks"] / rec["impressions"] * 100, 2) if rec["impressions"] > 0 else 0
        rec["cpc"] = round(rec["spend"] / rec["clicks"], 2) if rec["clicks"] > 0 else 0
        rec["conversion_rate"] = round(rec["conversions"] / rec["clicks"] * 100, 2) if rec["clicks"] > 0 else 0
        rec["roas"] = round(rec["revenue"] / rec["spend"], 2) if rec["spend"] > 0 else 0

    elapsed = round(time.time() - stage_start, 3)
    report = {"stage": "transformation", "records_transformed": len(records), "duration_s": elapsed}
    logger.info(f"Stage 3 complete: {len(records)} records transformed in {elapsed}s")
    return records, report


# ---------------------------------------------------------------------------
# Stage 4: Loading
# ---------------------------------------------------------------------------

def load_records(records):
    """Load validated and transformed records into the database."""
    stage_start = time.time()
    logger.info(f"Stage 4 - Loading {len(records)} records into database")

    # Collect unique channels and campaigns
    channel_ids = {}
    campaign_ids = {}

    for rec in records:
        ch_key = rec["channel"]
        if ch_key not in channel_ids:
            channel_ids[ch_key] = insert_channel(ch_key, rec["channel_category"])

    for rec in records:
        camp_key = rec["campaign_name"]
        if camp_key not in campaign_ids:
            campaign_ids[camp_key] = insert_campaign(
                campaign_name=rec["campaign_name"],
                channel_id=channel_ids[rec["channel"]],
                start_date=rec["start_date"],
                end_date=rec["end_date"],
                budget=rec["budget"],
                status="completed" if rec["end_date"] < datetime.now().strftime("%Y-%m-%d") else "active",
            )

    # Prepare daily metric tuples
    metric_tuples = []
    for rec in records:
        metric_tuples.append((
            campaign_ids[rec["campaign_name"]],
            rec["date"],
            rec["spend"],
            rec["impressions"],
            rec["clicks"],
            rec["conversions"],
            rec["revenue"],
        ))

    loaded = insert_daily_metrics(metric_tuples)

    elapsed = round(time.time() - stage_start, 3)
    report = {
        "stage": "loading",
        "channels_created": len(channel_ids),
        "campaigns_created": len(campaign_ids),
        "metrics_loaded": loaded,
        "duration_s": elapsed,
    }
    logger.info(f"Stage 4 complete: {loaded} metric records loaded in {elapsed}s")
    return report


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(num_campaigns=20, days=90):
    """Execute the full ETL pipeline: Generate -> Validate -> Transform -> Load."""
    global pipeline_status

    pipeline_status["status"] = "running"
    pipeline_status["stages"] = {}
    total_start = time.time()

    logger.info("=" * 60)
    logger.info("PIPELINE START")
    logger.info("=" * 60)

    # Reset and reinitialize database
    reset_db()
    init_db()

    # Stage 1: Generate
    records, gen_report = generate_sample_data(num_campaigns, days)
    pipeline_status["stages"]["generation"] = gen_report

    # Stage 2: Validate
    valid_records, rejected_records, val_report = validate_records(records)
    pipeline_status["stages"]["validation"] = val_report

    # Stage 3: Transform
    transformed_records, trans_report = transform_records(valid_records)
    pipeline_status["stages"]["transformation"] = trans_report

    # Stage 4: Load
    load_report = load_records(transformed_records)
    pipeline_status["stages"]["loading"] = load_report

    total_elapsed = round(time.time() - total_start, 3)

    pipeline_status.update({
        "last_run": datetime.now().isoformat(),
        "status": "completed",
        "records_generated": len(records),
        "records_validated": len(valid_records),
        "records_rejected": len(rejected_records),
        "records_loaded": load_report["metrics_loaded"],
        "duration_seconds": total_elapsed,
    })

    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE in {total_elapsed}s")
    logger.info(f"  Generated:  {len(records)}")
    logger.info(f"  Validated:  {len(valid_records)}")
    logger.info(f"  Rejected:   {len(rejected_records)}")
    logger.info(f"  Loaded:     {load_report['metrics_loaded']}")
    logger.info("=" * 60)

    return pipeline_status


def get_pipeline_status():
    return pipeline_status
