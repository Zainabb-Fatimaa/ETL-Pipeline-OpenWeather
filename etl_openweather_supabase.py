# etl_openweather_supabase.py
import os
import requests
from datetime import datetime, timezone
from supabase import create_client
from dotenv import load_dotenv
import sys

load_dotenv()

OPENWEATHER_KEY = os.getenv("OPENWEATHER_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")  # use service role (server-side)
CITY = os.getenv("CITY")
LAT = os.getenv("LAT")
LON = os.getenv("LON")

if not OPENWEATHER_KEY or not SUPABASE_URL or not SUPABASE_KEY:
    print("Missing env vars. Set OPENWEATHER_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY")
    sys.exit(1)

session = requests.Session()
session.headers.update({"Accept": "application/json"})

if LAT and LON:
    url = "http://api.openweathermap.org/data/2.5/weather"
    params = {"lat": LAT, "lon": LON, "appid": OPENWEATHER_KEY, "units": "metric"}
else:
    url = "http://api.openweathermap.org/data/2.5/weather"
    params = {"q": CITY, "appid": OPENWEATHER_KEY, "units": "metric"}

resp = session.get(url, params=params, timeout=15)
resp.raise_for_status()
j = resp.json()

# parse
ts = datetime.fromtimestamp(j["dt"], tz=timezone.utc).isoformat()
temp = j["main"]["temp"]
humidity = j["main"]["humidity"]
wind_speed = j.get("wind", {}).get("speed")
lat = j["coord"]["lat"]
lon = j["coord"]["lon"]
city = j.get("name", CITY)

# payload for supabase
payload = {
    "city": city,
    "latitude": lat,
    "longitude": lon,
    "timestamp": ts,
    "temperature": temp,
    "humidity": humidity,
    "wind_speed": wind_speed
}

# insert into supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
res = supabase.table("weather_data").insert(payload).execute()
print("Supabase response:", res)
