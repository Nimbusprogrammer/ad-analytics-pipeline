import sqlite3

DB_NAME = "analytics.db"

def get_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS campaigns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_name TEXT,
            channel TEXT,
            date TEXT,
            spend REAL,
            impressions INTEGER,
            clicks INTEGER,
            conversions INTEGER
        )
    """)
    
    conn.commit()
    conn.close()
    print("Database initialized.")

def insert_campaigns(records):
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.executemany("""
        INSERT INTO campaigns 
        (campaign_name, channel, date, spend, impressions, clicks, conversions)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, records)
    
    conn.commit()
    conn.close()
    print(f"Inserted {len(records)} records.")

def fetch_all():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM campaigns")
    rows = cursor.fetchall()
    conn.close()
    return rows

def fetch_summary():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            channel,
            ROUND(SUM(spend), 2) as total_spend,
            SUM(impressions) as total_impressions,
            SUM(clicks) as total_clicks,
            SUM(conversions) as total_conversions,
            ROUND(SUM(conversions) * 100.0 / SUM(clicks), 2) as conversion_rate,
            ROUND(SUM(spend) / SUM(conversions), 2) as cost_per_conversion
        FROM campaigns
        GROUP BY channel
        ORDER BY total_spend DESC
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows