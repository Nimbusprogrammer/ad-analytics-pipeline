from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from database import init_db, fetch_all, fetch_summary
from pipeline import run_pipeline

app = FastAPI(
    title="Ad Campaign Analytics Pipeline",
    description="ETL pipeline and REST API for marketing campaign performance analysis",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup():
    init_db()
    run_pipeline()
    print("Server ready.")

@app.get("/")
def root():
    return {
        "message": "Ad Campaign Analytics API",
        "endpoints": [
            "/campaigns",
            "/summary",
            "/summary/best-channel",
            "/summary/worst-channel",
            "/health"
        ]
    }

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/campaigns")
def get_campaigns():
    rows = fetch_all()
    if not rows:
        raise HTTPException(status_code=404, detail="No campaign data found")
    
    campaigns = []
    for row in rows:
        campaigns.append({
            "id": row[0],
            "campaign_name": row[1],
            "channel": row[2],
            "date": row[3],
            "spend": row[4],
            "impressions": row[5],
            "clicks": row[6],
            "conversions": row[7]
        })
    return {"total": len(campaigns), "campaigns": campaigns}

@app.get("/summary")
def get_summary():
    rows = fetch_summary()
    if not rows:
        raise HTTPException(status_code=404, detail="No summary data found")
    
    summary = []
    for row in rows:
        summary.append({
            "channel": row[0],
            "total_spend": row[1],
            "total_impressions": row[2],
            "total_clicks": row[3],
            "total_conversions": row[4],
            "conversion_rate_percent": row[5],
            "cost_per_conversion": row[6]
        })
    return {"summary": summary}

@app.get("/summary/best-channel")
def best_channel():
    rows = fetch_summary()
    if not rows:
        raise HTTPException(status_code=404, detail="No data found")
    
    best = min(rows, key=lambda x: x[6])
    return {
        "best_channel": best[0],
        "cost_per_conversion": best[6],
        "total_conversions": best[4],
        "total_spend": best[1]
    }

@app.get("/summary/worst-channel")
def worst_channel():
    rows = fetch_summary()
    if not rows:
        raise HTTPException(status_code=404, detail="No data found")
    
    worst = max(rows, key=lambda x: x[6])
    return {
        "worst_channel": worst[0],
        "cost_per_conversion": worst[6],
        "total_conversions": worst[4],
        "total_spend": worst[1]
    }