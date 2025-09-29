import requests, json, os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
url = f"http://api.openweathermap.org/data/2.5/weather?q=Karachi&appid={API_KEY}&units=metric"

resp = requests.get(url)
data = resp.json()

if resp.status_code != 200 or "main" not in data:
    print("Error fetching data:", data)
else:
    record = {
        "city": "Karachi",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature_c": data["main"]["temp"],        # already in °C
        "humidity_pct": data["main"]["humidity"],     # %
        "wind_ms": data["wind"]["speed"]              # m/s
    }

    os.makedirs("data", exist_ok=True)
    with open("data/karachi_weather.json", "w") as f:
        json.dump(record, f, indent=2)

    print("Karachi weather saved as JSON ")
