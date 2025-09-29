import json, csv, sqlite3, psycopg2, os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# --- 1. Extract ---
# Karachi JSON
with open("data/karachi_weather.json") as f:
    karachi = json.load(f)

# Lahore CSV
with open("data/lahore_weather.csv") as f:
    reader = csv.DictReader(f)
    lahore = next(reader)

# Islamabad SQLite
conn = sqlite3.connect("data/islamabad_weather.sqlite")
df_sql = pd.read_sql(
    "SELECT * FROM islamabad_weather ORDER BY ROWID DESC LIMIT 1", conn
)
islamabad = df_sql.to_dict(orient="records")[0]
conn.close()

# --- 2. Transform (normalize schema & units) ---
def normalize(record):
    return {
        "city": record["city"],
        "timestamp": record["timestamp"],
        "temperature_c": float(record["temperature_c"]),
        "humidity_pct": float(record["humidity_pct"]),
        "wind_ms": float(record["wind_ms"]),
    }

records = [normalize(karachi), normalize(lahore), normalize(islamabad)]

# --- 3. OLAP Aggregation ---
df = pd.DataFrame(records)
agg = df.groupby("city").agg(
    {
        "temperature_c": ["min", "max", "mean"],
        "humidity_pct": "mean",
        "wind_ms": "mean",
    }
).reset_index()

agg.columns = [
    "city",
    "temp_min",
    "temp_max",
    "temp_avg",
    "humidity_avg",
    "wind_avg",
]

# --- 4. Load into Postgres (Supabase via transaction pooling) ---
USER = os.getenv("SUPABASE_USER")         
PASSWORD = os.getenv("SUPABASE_PASSWORD")  
HOST = os.getenv("SUPABASE_POOL_HOST", "aws-1-ap-southeast-1.pooler.supabase.com")
PORT = os.getenv("SUPABASE_POOL_PORT", "6543")
DBNAME = os.getenv("SUPABASE_DB", "postgres")

try:
    conn = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME,
        sslmode="require"   # Supabase requires SSL
    )
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS weather_agg (
            city TEXT,
            temp_min REAL,
            temp_max REAL,
            temp_avg REAL,
            humidity_avg REAL,
            wind_avg REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    for _, row in agg.iterrows():
        cur.execute(
            """
            INSERT INTO weather_agg (city, temp_min, temp_max, temp_avg, humidity_avg, wind_avg)
            VALUES (%s,%s,%s,%s,%s,%s)
            """,
            tuple(row.values),
        )

    conn.commit()
    cur.close()
    conn.close()
    print("Aggregated data loaded into Supabase Postgres (via transaction pooling)")

except Exception as e:
    print(f" Failed to connect or insert: {e}")
