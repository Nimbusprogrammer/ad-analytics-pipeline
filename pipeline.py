import pandas as pd
import random
from datetime import datetime, timedelta
from database import insert_campaigns

CHANNELS = ["Google Ads", "Facebook", "Instagram", "YouTube", "Email"]
CAMPAIGNS = ["Summer Sale", "Brand Awareness", "Product Launch", "Retargeting", "Holiday Push"]

def generate_sample_data(num_records=200):
    records = []
    start_date = datetime(2024, 1, 1)

    for _ in range(num_records):
        channel = random.choice(CHANNELS)
        campaign = random.choice(CAMPAIGNS)
        date = start_date + timedelta(days=random.randint(0, 365))
        spend = round(random.uniform(100, 10000), 2)
        impressions = random.randint(1000, 500000)
        clicks = random.randint(50, int(impressions * 0.1))
        conversions = random.randint(1, int(clicks * 0.3))

        records.append((
            campaign,
            channel,
            date.strftime("%Y-%m-%d"),
            spend,
            impressions,
            clicks,
            conversions
        ))

    return records

def load_from_csv(filepath):
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.strip().str.lower()

    required = ["campaign_name", "channel", "date", "spend", "impressions", "clicks", "conversions"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    df["spend"] = pd.to_numeric(df["spend"], errors="coerce").fillna(0)
    df["impressions"] = pd.to_numeric(df["impressions"], errors="coerce").fillna(0).astype(int)
    df["clicks"] = pd.to_numeric(df["clicks"], errors="coerce").fillna(0).astype(int)
    df["conversions"] = pd.to_numeric(df["conversions"], errors="coerce").fillna(0).astype(int)

    records = list(df[required].itertuples(index=False, name=None))
    return records

def run_pipeline(csv_path=None):
    print("Starting ETL pipeline...")

    if csv_path:
        print(f"Loading data from {csv_path}")
        records = load_from_csv(csv_path)
    else:
        print("Generating sample data...")
        records = generate_sample_data(200)

    print(f"Transforming {len(records)} records...")
    insert_campaigns(records)
    print("Pipeline complete.")