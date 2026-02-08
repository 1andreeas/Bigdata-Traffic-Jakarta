import random
from datetime import datetime, timedelta
from config import (
    LOCATIONS,
    VEHICLE_PATTERN,
    RAIN_IMPACT,
    TRAFFIC_THRESHOLDS,
    HISTORICAL_DAYS,
    DATA_INTERVAL_MINUTES,
)
from database import TrafficDatabase


class DataGenerator:
    def __init__(self):
        self.db = TrafficDatabase()

    def simulate_historical_weather(self, hour: int, day_of_week: int) -> dict:
        rain_probability = 0.15

        if 6 <= hour <= 10:
            rain_probability = 0.45
        elif 15 <= hour <= 18:
            rain_probability = 0.40
        elif 0 <= hour <= 5:
            rain_probability = 0.10

        rain_probability *= 1.2

        is_rain = random.random() < rain_probability

        if is_rain:
            intensity = random.random()
            if intensity < 0.5:
                rain_cat = "light"
                precipitation = round(random.uniform(0.5, 2.5), 2)
            elif intensity < 0.8:
                rain_cat = "moderate"
                precipitation = round(random.uniform(2.5, 7.0), 2)
            elif intensity < 0.95:
                rain_cat = "heavy"
                precipitation = round(random.uniform(7.0, 15.0), 2)
            else:
                rain_cat = "extreme"
                precipitation = round(random.uniform(15.0, 30.0), 2)
        else:
            rain_cat = "none"
            precipitation = 0.0

        if 5 <= hour <= 10:
            temperature = round(random.uniform(25, 29), 1)
        elif 10 <= hour <= 15:
            temperature = round(random.uniform(29, 33), 1)
        elif 15 <= hour <= 20:
            temperature = round(random.uniform(27, 31), 1)
        else:
            temperature = round(random.uniform(24, 28), 1)

        return {
            "precipitation": precipitation,
            "rain_category": rain_cat,
            "temperature": temperature,
        }

    def get_condition(self, vehicle_count: int) -> str:
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

    def generate_historical_data(self):
        print("=" * 50)
        print("🏭 GENERATING HISTORICAL DATA...")
        print("=" * 50)

        now = datetime.now()
        start_date = now - timedelta(days=HISTORICAL_DAYS)

        traffic_batch = []
        weather_batch = []

        total_expected = HISTORICAL_DAYS * (24 * 60 // DATA_INTERVAL_MINUTES) * len(LOCATIONS)
        count = 0

        print(f"📊 Target: {total_expected:,} baris data traffic")
        print(f"📅 Dari: {start_date.strftime('%Y-%m-%d')} sampai {now.strftime('%Y-%m-%d')}")
        print(f"📍 Lokasi: {len(LOCATIONS)} titik")
        print("─" * 50)

        current_date = start_date
        while current_date < now:
            day_of_week = current_date.weekday()

            current_time = current_date.replace(hour=0, minute=0, second=0)
            end_of_day = current_date.replace(hour=23, minute=59, second=59)

            weather_cache = {}

            while current_time <= end_of_day and current_time < now:
                hour = current_time.hour

                if hour not in weather_cache:
                    weather_cache[hour] = self.simulate_historical_weather(hour, day_of_week)

                weather = weather_cache[hour]

                for location in LOCATIONS:
                    base_vehicles = VEHICLE_PATTERN.get(hour, 100)

                    location_var = random.uniform(0.8, 1.2)

                    if day_of_week >= 5:
                        day_var = random.uniform(0.6, 0.85)
                    else:
                        day_var = random.uniform(0.9, 1.1)

                    rain_factor = RAIN_IMPACT.get(weather["rain_category"], 1.0)
                    vehicles = int(base_vehicles * location_var * day_var * rain_factor)

                    condition = self.get_condition(vehicles)
                    speed = self.calculate_speed(vehicles, rain_factor)

                    is_peak = 1 if (6 <= hour <= 8 or 16 <= hour <= 18) else 0

                    traffic_batch.append({
                        "timestamp": current_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "location": location,
                        "vehicle_count": vehicles,
                        "condition": condition,
                        "speed_kmh": speed,
                        "hour": hour,
                        "is_peak": is_peak,
                        "rain_factor": rain_factor,
                        "data_source": "historical_generated",
                    })

                    count += 1

                if current_time.minute == 0:
                    for location in LOCATIONS:
                        weather_batch.append({
                            "timestamp": current_time.strftime("%Y-%m-%d %H:%M:%S"),
                            "location": location,
                            "temperature": weather["temperature"],
                            "precipitation": weather["precipitation"],
                            "windspeed": round(random.uniform(5, 25), 1),
                            "weather_code": 61 if weather["rain_category"] != "none" else 0,
                            "weather_desc": "Hujan" if weather["rain_category"] != "none" else "Cerah",
                            "rain_category": weather["rain_category"],
                        })

                if count % 10000 == 0:
                    print(f"  📝 Progress: {count:,} / {total_expected:,} baris "
                          f"({count/total_expected*100:.1f}%)")

                current_time += timedelta(minutes=DATA_INTERVAL_MINUTES)

            current_date += timedelta(days=1)

            if len(traffic_batch) > 5000:
                self.db.insert_traffic_data(traffic_batch)
                traffic_batch = []

        if traffic_batch:
            self.db.insert_traffic_data(traffic_batch)

        print(f"\n💧 Saving {len(weather_batch):,} weather records...")
        batch_size = 1000
        for i in range(0, len(weather_batch), batch_size):
            batch = weather_batch[i:i + batch_size]
            conn = self.db.get_connection()
            cursor = conn.cursor()
            cursor.executemany("""
                INSERT INTO weather_data 
                (timestamp, location, temperature, precipitation, windspeed,
                 weather_code, weather_desc, rain_category)
                VALUES 
                (:timestamp, :location, :temperature, :precipitation, :windspeed,
                 :weather_code, :weather_desc, :rain_category)
            """, batch)
            conn.commit()
            conn.close()

        print("\n" + "=" * 50)
        print("✅ HISTORICAL DATA GENERATION COMPLETE!")
        print("=" * 50)
        total_traffic = self.db.get_traffic_count()
        total_weather = self.db.get_weather_count()
        print(f"📊 Total traffic records: {total_traffic:,}")
        print(f"🌤️  Total weather records: {total_weather:,}")
        print("=" * 50)