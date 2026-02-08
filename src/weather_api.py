import requests
from datetime import datetime
from config import WEATHER_API_URL, LOCATIONS, WEATHER_PARAMS
from database import TrafficDatabase


class WeatherAPI:
    def __init__(self):
        self.api_url = WEATHER_API_URL
        self.locations = LOCATIONS
        self.db = TrafficDatabase()

    def decode_weather_code(self, code: int) -> dict:
        weather_map = {
            0:  {"description": "Cerah",              "rain_category": "none"},
            1:  {"description": "Cerah Sebagian",     "rain_category": "none"},
            2:  {"description": "Berawan Sebagian",   "rain_category": "none"},
            3:  {"description": "Mendung",            "rain_category": "none"},
            45: {"description": "Kabut",              "rain_category": "none"},
            48: {"description": "Kabut Tebal",        "rain_category": "none"},
            51: {"description": "Gerimis Ringan",     "rain_category": "light"},
            53: {"description": "Gerimis Sedang",     "rain_category": "light"},
            55: {"description": "Gerimis Lebat",      "rain_category": "moderate"},
            61: {"description": "Hujan Ringan",       "rain_category": "light"},
            63: {"description": "Hujan Sedang",       "rain_category": "moderate"},
            65: {"description": "Hujan Lebat",        "rain_category": "heavy"},
            66: {"description": "Hujan Es Ringan",    "rain_category": "moderate"},
            67: {"description": "Hujan Es Lebat",     "rain_category": "heavy"},
            71: {"description": "Salju Ringan",       "rain_category": "light"},
            73: {"description": "Salju Sedang",       "rain_category": "moderate"},
            75: {"description": "Salju Lebat",        "rain_category": "heavy"},
            80: {"description": "Hujan Singkat",      "rain_category": "light"},
            81: {"description": "Hujan Singkat Sedang","rain_category": "moderate"},
            82: {"description": "Hujan Singkat Lebat","rain_category": "heavy"},
            95: {"description": "Petir",              "rain_category": "extreme"},
            96: {"description": "Petir + Es",         "rain_category": "extreme"},
            99: {"description": "Petir + Es Lebat",   "rain_category": "extreme"},
        }
        return weather_map.get(code, {"description": "Unknown", "rain_category": "none"})

    def get_weather(self, location: str) -> dict:
        if location not in self.locations:
            print(f"❌ Lokasi '{location}' tidak ditemukan!")
            return None

        coords = self.locations[location]
        params = WEATHER_PARAMS.copy()
        params["latitude"] = coords["lat"]
        params["longitude"] = coords["lon"]

        try:
            print(f"🌤️  Fetching cuaca untuk {location}...")
            response = requests.get(self.api_url, params=params, timeout=10)

            if response.status_code != 200:
                print(f"❌ API error: status {response.status_code}")
                return None

            data = response.json()
            current = data.get("current_weather", {})
            hourly = data.get("hourly", {})

            current_hour = datetime.now().hour
            precipitation = 0.0

            if "precipitation" in hourly:
                times = hourly.get("time", [])
                for i, time_str in enumerate(times):
                    if datetime.fromisoformat(time_str).hour == current_hour:
                        precipitation = hourly["precipitation"][i]
                        break

            weather_code = current.get("weathercode", 0)
            weather_info = self.decode_weather_code(weather_code)

            result = {
                "location": location,
                "temperature": current.get("temperature", 0),
                "precipitation": precipitation,
                "windspeed": current.get("windspeed", 0),
                "weather_code": weather_code,
                "weather_desc": weather_info["description"],
                "rain_category": weather_info["rain_category"],
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            print(f"  ✅ {location}: {result['weather_desc']}, "
                  f"Suhu {result['temperature']}°C, "
                  f"Hujan {result['precipitation']}mm")

            return result

        except requests.exceptions.ConnectionError:
            print(f"❌ Tidak bisa konek ke internet! Cek koneksi.")
            return None
        except requests.exceptions.Timeout:
            print(f"❌ Request timeout! API lambat.")
            return None
        except Exception as e:
            print(f"❌ Error tak terduga: {e}")
            return None

    def get_all_weather(self) -> list:
        results = []
        print("\n🌍 Fetching cuaca semua lokasi Jakarta...")
        print("─" * 40)

        for location in self.locations:
            data = self.get_weather(location)
            if data:
                results.append(data)

        print("─" * 40)
        print(f"✅ Berhasil fetch {len(results)} lokasi\n")
        return results

    def fetch_and_save(self) -> list:
        weather_list = self.get_all_weather()

        for weather in weather_list:
            self.db.insert_weather_data(weather)

        print(f"💾 Saved {len(weather_list)} weather records to database")
        return weather_list