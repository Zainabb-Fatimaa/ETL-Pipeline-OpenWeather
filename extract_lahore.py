import requests, csv, os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("WEATHERAPI_KEY")
url = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q=Lahore"
resp = requests.get(url).json()
cw = resp["current"]

record = {
    "city": "Lahore",
    "timestamp": resp["location"]["localtime"],
    "temperature_c": cw["temp_c"],         # °C
    "humidity_pct": cw["humidity"],        # %
    "wind_ms": cw["wind_kph"] / 3.6        # km/h → m/s
}

with open("data/lahore_weather.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=record.keys())
    writer.writeheader()
    writer.writerow(record)

print(" Lahore weather saved as CSV (with humidity from WeatherAPI)")
