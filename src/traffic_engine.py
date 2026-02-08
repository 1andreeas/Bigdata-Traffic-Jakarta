import random
from datetime import datetime
from config import (
    LOCATIONS,
    VEHICLE_PATTERN,
    RAIN_IMPACT,
    TRAFFIC_THRESHOLDS,
    PEAK_MORNING,
    PEAK_EVENING,
)
from database import TrafficDatabase
from weather_api import WeatherAPI


class TrafficEngine:
    def __init__(self):
        self.db = TrafficDatabase()
        self.weather_api = WeatherAPI()
        self.last_weather = {}

    def is_peak_hour(self, hour: int) -> bool:
        is_morning_peak = PEAK_MORNING["start"] <= hour < PEAK_MORNING["end"]
        is_evening_peak = PEAK_EVENING["start"] <= hour < PEAK_EVENING["end"]
        return is_morning_peak or is_evening_peak

    def get_traffic_condition(self, vehicle_count: int) -> str:
        for condition, (low, high) in TRAFFIC_THRESHOLDS.items():
            if low <= vehicle_count < high:
                return condition
        return "Macet"

    def calculate_speed(self, vehicle_count: int, rain_factor: float) -> float:
        max_speed = 60.0
        min_speed = 5.0

        speed = max_speed - (vehicle_count / 10.0)
        speed = speed / rain_factor
        speed += random.uniform(-3, 3)
        speed = max(min_speed, min(max_speed, speed))

        return round(speed, 1)

    def simulate_location(self, location: str, weather_data: dict = None) -> dict:
        now = datetime.now()
        hour = now.hour

        base_vehicles = VEHICLE_PATTERN.get(hour, 100)
        location_variance = random.uniform(0.8, 1.2)
        vehicles = int(base_vehicles * location_variance)

        rain_factor = 1.0

        if weather_data:
            rain_cat = weather_data.get("rain_category", "none")
            rain_factor = RAIN_IMPACT.get(rain_cat, 1.0)
            vehicles = int(vehicles * rain_factor)

        condition = self.get_traffic_condition(vehicles)
        speed = self.calculate_speed(vehicles, rain_factor)
        is_peak = 1 if self.is_peak_hour(hour) else 0

        result = {
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            "location": location,
            "vehicle_count": vehicles,
            "condition": condition,
            "speed_kmh": speed,
            "hour": hour,
            "is_peak": is_peak,
            "rain_factor": rain_factor,
            "data_source": "real_time_simulated",
        }

        return result

    def run_simulation_cycle(self) -> list:
        print("\n🔄 Running simulation cycle...")
        print("─" * 40)

        weather_list = self.weather_api.fetch_and_save()

        weather_map = {}
        for w in weather_list:
            weather_map[w["location"]] = w

        traffic_records = []

        for location in LOCATIONS:
            weather = weather_map.get(location)
            traffic = self.simulate_location(location, weather)
            traffic_records.append(traffic)

            emoji = "🟢" if traffic["condition"] == "Lancar" else \
                    "🟡" if traffic["condition"] == "Sedang" else \
                    "🟠" if traffic["condition"] == "Padat" else \
                    "🔴"
            print(f"  {emoji} {location}: "
                  f"{traffic['vehicle_count']} kendaraan, "
                  f"{traffic['condition']}, "
                  f"{traffic['speed_kmh']} km/h")

        self.db.insert_traffic_data(traffic_records)

        print("─" * 40)
        print(f"✅ Simulation cycle complete! {len(traffic_records)} records saved\n")

        return traffic_records