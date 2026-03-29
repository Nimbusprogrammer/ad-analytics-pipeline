import sqlite3
import os

DB_NAME = os.getenv("DATABASE_PATH", "analytics.db")


def get_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            category TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS campaigns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_name TEXT NOT NULL,
            channel_id INTEGER NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            budget REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            FOREIGN KEY (channel_id) REFERENCES channels(id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            spend REAL NOT NULL,
            impressions INTEGER NOT NULL,
            clicks INTEGER NOT NULL,
            conversions INTEGER NOT NULL,
            revenue REAL NOT NULL DEFAULT 0,
            FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
            UNIQUE(campaign_id, date)
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_date ON daily_metrics(date)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_campaign ON daily_metrics(campaign_id)
    """)

    conn.commit()
    conn.close()
    print("Database initialized with normalized schema.")


def reset_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS daily_metrics")
    cursor.execute("DROP TABLE IF EXISTS campaigns")
    cursor.execute("DROP TABLE IF EXISTS channels")
    conn.commit()
    conn.close()


def insert_channel(name, category):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO channels (name, category) VALUES (?, ?)",
        (name, category),
    )
    conn.commit()
    cursor.execute("SELECT id FROM channels WHERE name = ?", (name,))
    row = cursor.fetchone()
    conn.close()
    return row["id"]


def insert_campaign(campaign_name, channel_id, start_date, end_date, budget, status="active"):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO campaigns (campaign_name, channel_id, start_date, end_date, budget, status)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (campaign_name, channel_id, start_date, end_date, budget, status),
    )
    conn.commit()
    campaign_id = cursor.lastrowid
    conn.close()
    return campaign_id


def insert_daily_metrics(records):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.executemany(
        """INSERT OR REPLACE INTO daily_metrics
           (campaign_id, date, spend, impressions, clicks, conversions, revenue)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        records,
    )
    conn.commit()
    conn.close()
    return len(records)


def fetch_all_campaigns():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            c.id,
            c.campaign_name,
            ch.name as channel,
            ch.category,
            c.start_date,
            c.end_date,
            c.budget,
            c.status,
            COALESCE(SUM(dm.spend), 0) as total_spend,
            COALESCE(SUM(dm.impressions), 0) as total_impressions,
            COALESCE(SUM(dm.clicks), 0) as total_clicks,
            COALESCE(SUM(dm.conversions), 0) as total_conversions,
            COALESCE(SUM(dm.revenue), 0) as total_revenue
        FROM campaigns c
        JOIN channels ch ON c.channel_id = ch.id
        LEFT JOIN daily_metrics dm ON c.id = dm.campaign_id
        GROUP BY c.id
        ORDER BY total_spend DESC
    """)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def fetch_campaign_detail(campaign_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            c.id, c.campaign_name, ch.name as channel, ch.category,
            c.start_date, c.end_date, c.budget, c.status
        FROM campaigns c
        JOIN channels ch ON c.channel_id = ch.id
        WHERE c.id = ?
    """, (campaign_id,))
    campaign = cursor.fetchone()

    if not campaign:
        conn.close()
        return None

    campaign = dict(campaign)

    cursor.execute("""
        SELECT date, spend, impressions, clicks, conversions, revenue
        FROM daily_metrics
        WHERE campaign_id = ?
        ORDER BY date
    """, (campaign_id,))
    campaign["daily_metrics"] = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return campaign


def fetch_campaign_trends(campaign_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            dm.date,
            dm.spend,
            dm.impressions,
            dm.clicks,
            dm.conversions,
            dm.revenue,
            CASE WHEN dm.clicks > 0
                THEN ROUND(dm.conversions * 100.0 / dm.clicks, 2)
                ELSE 0 END as conversion_rate,
            CASE WHEN dm.impressions > 0
                THEN ROUND(dm.clicks * 100.0 / dm.impressions, 2)
                ELSE 0 END as ctr,
            CASE WHEN dm.clicks > 0
                THEN ROUND(dm.spend / dm.clicks, 2)
                ELSE 0 END as cpc
        FROM daily_metrics dm
        WHERE dm.campaign_id = ?
        ORDER BY dm.date
    """, (campaign_id,))
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def fetch_channel_comparison():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            ch.name as channel,
            ch.category,
            COUNT(DISTINCT c.id) as campaign_count,
            ROUND(SUM(dm.spend), 2) as total_spend,
            SUM(dm.impressions) as total_impressions,
            SUM(dm.clicks) as total_clicks,
            SUM(dm.conversions) as total_conversions,
            ROUND(SUM(dm.revenue), 2) as total_revenue,
            ROUND(SUM(dm.revenue) - SUM(dm.spend), 2) as net_profit,
            CASE WHEN SUM(dm.spend) > 0
                THEN ROUND((SUM(dm.revenue) - SUM(dm.spend)) * 100.0 / SUM(dm.spend), 2)
                ELSE 0 END as roi_percent,
            CASE WHEN SUM(dm.clicks) > 0
                THEN ROUND(SUM(dm.conversions) * 100.0 / SUM(dm.clicks), 2)
                ELSE 0 END as conversion_rate,
            CASE WHEN SUM(dm.conversions) > 0
                THEN ROUND(SUM(dm.spend) / SUM(dm.conversions), 2)
                ELSE 0 END as cost_per_conversion,
            CASE WHEN SUM(dm.impressions) > 0
                THEN ROUND(SUM(dm.clicks) * 100.0 / SUM(dm.impressions), 2)
                ELSE 0 END as ctr
        FROM channels ch
        JOIN campaigns c ON c.channel_id = ch.id
        JOIN daily_metrics dm ON dm.campaign_id = c.id
        GROUP BY ch.id
        ORDER BY total_revenue DESC
    """)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def fetch_daily_totals(start_date=None, end_date=None):
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT
            dm.date,
            ROUND(SUM(dm.spend), 2) as total_spend,
            SUM(dm.impressions) as total_impressions,
            SUM(dm.clicks) as total_clicks,
            SUM(dm.conversions) as total_conversions,
            ROUND(SUM(dm.revenue), 2) as total_revenue
        FROM daily_metrics dm
    """
    params = []

    conditions = []
    if start_date:
        conditions.append("dm.date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("dm.date <= ?")
        params.append(end_date)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " GROUP BY dm.date ORDER BY dm.date"

    cursor.execute(query, params)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def fetch_anomalies(threshold=2.0):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        WITH channel_stats AS (
            SELECT
                ch.name as channel,
                AVG(
                    CASE WHEN dm.conversions > 0
                    THEN dm.spend / dm.conversions
                    ELSE NULL END
                ) as avg_cpc,
                -- Population standard deviation calculated manually
                -- stddev = sqrt(avg(x^2) - avg(x)^2)
                SQRT(
                    AVG(
                        CASE WHEN dm.conversions > 0
                        THEN (dm.spend / dm.conversions) * (dm.spend / dm.conversions)
                        ELSE NULL END
                    ) -
                    AVG(
                        CASE WHEN dm.conversions > 0
                        THEN dm.spend / dm.conversions
                        ELSE NULL END
                    ) *
                    AVG(
                        CASE WHEN dm.conversions > 0
                        THEN dm.spend / dm.conversions
                        ELSE NULL END
                    )
                ) as stddev_cpc
            FROM daily_metrics dm
            JOIN campaigns c ON dm.campaign_id = c.id
            JOIN channels ch ON c.channel_id = ch.id
            GROUP BY ch.id
        ),
        flagged AS (
            SELECT
                dm.id,
                dm.date,
                c.campaign_name,
                ch.name as channel,
                dm.spend,
                dm.impressions,
                dm.clicks,
                dm.conversions,
                dm.revenue,
                CASE WHEN dm.conversions > 0
                    THEN ROUND(dm.spend / dm.conversions, 2)
                    ELSE NULL END as cost_per_conv,
                cs.avg_cpc,
                cs.stddev_cpc,
                CASE
                    WHEN dm.conversions = 0 AND dm.spend > 0 THEN 'zero_conversions'
                    WHEN dm.clicks > dm.impressions THEN 'clicks_exceed_impressions'
                    WHEN dm.conversions > 0 AND cs.stddev_cpc > 0 AND
                        ABS((dm.spend / dm.conversions) - cs.avg_cpc) > ? * cs.stddev_cpc
                        THEN 'cost_deviation'
                    WHEN dm.spend > 0 AND dm.impressions = 0 THEN 'spend_no_impressions'
                    ELSE NULL
                END as anomaly_type
            FROM daily_metrics dm
            JOIN campaigns c ON dm.campaign_id = c.id
            JOIN channels ch ON c.channel_id = ch.id
            JOIN channel_stats cs ON cs.channel = ch.name
        )
        SELECT
            id, date, campaign_name, channel,
            spend, impressions, clicks, conversions, revenue,
            cost_per_conv,
            ROUND(avg_cpc, 2) as channel_avg_cpc,
            ROUND(stddev_cpc, 2) as channel_stddev_cpc,
            anomaly_type
        FROM flagged
        WHERE anomaly_type IS NOT NULL
        ORDER BY date DESC
    """, (threshold,))

    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def fetch_kpi_summary():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            COUNT(DISTINCT c.id) as total_campaigns,
            COUNT(DISTINCT ch.id) as total_channels,
            COUNT(dm.id) as total_records,
            ROUND(SUM(dm.spend), 2) as total_spend,
            ROUND(SUM(dm.revenue), 2) as total_revenue,
            ROUND(SUM(dm.revenue) - SUM(dm.spend), 2) as net_profit,
            CASE WHEN SUM(dm.spend) > 0
                THEN ROUND((SUM(dm.revenue) - SUM(dm.spend)) * 100.0 / SUM(dm.spend), 2)
                ELSE 0 END as overall_roi,
            SUM(dm.impressions) as total_impressions,
            SUM(dm.clicks) as total_clicks,
            SUM(dm.conversions) as total_conversions,
            CASE WHEN SUM(dm.clicks) > 0
                THEN ROUND(SUM(dm.conversions) * 100.0 / SUM(dm.clicks), 2)
                ELSE 0 END as overall_conversion_rate,
            MIN(dm.date) as data_start_date,
            MAX(dm.date) as data_end_date
        FROM daily_metrics dm
        JOIN campaigns c ON dm.campaign_id = c.id
        JOIN channels ch ON c.channel_id = ch.id
    """)
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None
