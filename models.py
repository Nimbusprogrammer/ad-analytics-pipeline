from pydantic import BaseModel
from typing import Optional


class ChannelSummary(BaseModel):
    channel: str
    category: str
    campaign_count: int
    total_spend: float
    total_impressions: int
    total_clicks: int
    total_conversions: int
    total_revenue: float
    net_profit: float
    roi_percent: float
    conversion_rate: float
    cost_per_conversion: float
    ctr: float


class CampaignOverview(BaseModel):
    id: int
    campaign_name: str
    channel: str
    category: str
    start_date: str
    end_date: str
    budget: float
    status: str
    total_spend: float
    total_impressions: int
    total_clicks: int
    total_conversions: int
    total_revenue: float


class DailyMetric(BaseModel):
    date: str
    spend: float
    impressions: int
    clicks: int
    conversions: int
    revenue: float


class TrendPoint(BaseModel):
    date: str
    spend: float
    impressions: int
    clicks: int
    conversions: int
    revenue: float
    conversion_rate: float
    ctr: float
    cpc: float


class AnomalyRecord(BaseModel):
    id: int
    date: str
    campaign_name: str
    channel: str
    spend: float
    impressions: int
    clicks: int
    conversions: int
    revenue: float
    cost_per_conv: Optional[float]
    channel_avg_cpc: Optional[float]
    channel_stddev_cpc: Optional[float]
    anomaly_type: str


class DailyTotal(BaseModel):
    date: str
    total_spend: float
    total_impressions: int
    total_clicks: int
    total_conversions: int
    total_revenue: float


class KPISummary(BaseModel):
    total_campaigns: int
    total_channels: int
    total_records: int
    total_spend: float
    total_revenue: float
    net_profit: float
    overall_roi: float
    total_impressions: int
    total_clicks: int
    total_conversions: int
    overall_conversion_rate: float
    data_start_date: Optional[str]
    data_end_date: Optional[str]


class PipelineStatus(BaseModel):
    last_run: Optional[str]
    status: str
    records_generated: int
    records_validated: int
    records_rejected: int
    records_loaded: int
    duration_seconds: float
    stages: dict
