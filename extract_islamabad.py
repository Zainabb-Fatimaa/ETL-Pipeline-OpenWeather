import requests, sqlite3, os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("WEATHERAPI_KEY")
url = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q=Islamabad"
resp = requests.get(url).json()
cw = resp["current"]

record = {
    "city": "Islamabad",
    "timestamp": resp["location"]["localtime"],
    "temperature_c": cw["temp_c"],            # already °C
    "humidity_pct": cw["humidity"],           # %
    "wind_ms": cw["wind_kph"] / 3.6           # convert km/h → m/s
}

conn = sqlite3.connect("data/islamabad_weather.sqlite")
c = conn.cursor()
c.execute("""
CREATE TABLE IF NOT EXISTS islamabad_weather (
    city TEXT, timestamp TEXT, temperature_c REAL,
    humidity_pct REAL, wind_ms REAL
)
""")
c.execute("INSERT INTO islamabad_weather VALUES (?,?,?,?,?)",
          tuple(record.values()))
conn.commit()
conn.close()

print(" Islamabad weather saved into SQLite DB")
