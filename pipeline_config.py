CHANNELS = {
    "Google Ads": {"category": "paid", "conv_rate_range": (0.03, 0.08), "cpc_range": (0.5, 3.0)},
    "Facebook":   {"category": "social", "conv_rate_range": (0.02, 0.06), "cpc_range": (0.3, 2.5)},
    "Instagram":  {"category": "social", "conv_rate_range": (0.015, 0.05), "cpc_range": (0.4, 2.8)},
    "YouTube":    {"category": "paid", "conv_rate_range": (0.01, 0.04), "cpc_range": (0.2, 1.5)},
    "Email":      {"category": "organic", "conv_rate_range": (0.05, 0.12), "cpc_range": (0.1, 0.8)},
}

CAMPAIGNS = {
    "Summer Sale":      {"budget_range": (5000, 25000), "duration_days": (30, 60)},
    "Brand Awareness":  {"budget_range": (10000, 50000), "duration_days": (60, 90)},
    "Product Launch":   {"budget_range": (8000, 40000), "duration_days": (14, 45)},
    "Retargeting":      {"budget_range": (3000, 15000), "duration_days": (30, 60)},
    "Holiday Push":     {"budget_range": (15000, 60000), "duration_days": (20, 40)},
    "Back to School":   {"budget_range": (5000, 20000), "duration_days": (21, 35)},
    "Flash Sale":       {"budget_range": (2000, 10000), "duration_days": (3, 7)},
    "Year End Clearance": {"budget_range": (10000, 45000), "duration_days": (14, 30)},
}

VALIDATION_RULES = {
    "min_spend": 0,
    "max_spend_per_day": 50000,
    "max_impressions_per_day": 5000000,
    "min_ctr": 0,
    "max_ctr": 0.5,
}

ANOMALY_INJECTION_RATE = 0.03

REVENUE_PER_CONVERSION_RANGE = (5.0, 75.0)
