import pandas as pd
import numpy as np
from database import TrafficDatabase
from config import LOCATIONS


class TrafficAnalytics:
    def __init__(self):
        self.db = TrafficDatabase()

    def get_overall_stats(self) -> dict:
        df = self.db.get_all_traffic_data()

        if df.empty:
            return {"error": "Tidak ada data"}

        stats = {
            "total_records": len(df),
            "total_locations": df["location"].nunique(),
            "avg_vehicles": round(df["vehicle_count"].mean(), 1),
            "max_vehicles": int(df["vehicle_count"].max()),
            "min_vehicles": int(df["vehicle_count"].min()),
            "avg_speed": round(df["speed_kmh"].mean(), 1),
            "most_common_condition": df["condition"].mode()[0] if not df["condition"].mode().empty else "N/A",
            "peak_records": int(df["is_peak"].sum()),
            "rainy_records": int((df["rain_factor"] > 1.0).sum()),
        }

        return stats

    def get_hourly_pattern(self, location: str = None) -> pd.DataFrame:
        if location:
            df = self.db.get_traffic_by_location(location)
        else:
            df = self.db.get_all_traffic_data()

        if df.empty:
            return pd.DataFrame()

        hourly = df.groupby("hour").agg(
            avg_vehicles=("vehicle_count", "mean"),
            max_vehicles=("vehicle_count", "max"),
            avg_speed=("speed_kmh", "mean"),
            count=("id", "count"),
        ).reset_index()

        hourly["avg_vehicles"] = hourly["avg_vehicles"].round(1)
        hourly["avg_speed"] = hourly["avg_speed"].round(1)

        return hourly

    def get_rain_correlation(self) -> dict:
        df = self.db.get_all_traffic_data()

        if df.empty:
            return {"error": "Tidak ada data"}

        def categorize_rain(factor):
            if factor <= 1.0:
                return "Tidak Hujan"
            elif factor <= 1.3:
                return "Hujan Ringan"
            elif factor <= 1.6:
                return "Hujan Sedang"
            elif factor <= 1.8:
                return "Hujan Lebat"
            else:
                return "Hujan Ekstrem"

        df["rain_category"] = df["rain_factor"].apply(categorize_rain)

        rain_stats = df.groupby("rain_category").agg(
            avg_vehicles=("vehicle_count", "mean"),
            max_vehicles=("vehicle_count", "max"),
            avg_speed=("speed_kmh", "mean"),
            count=("id", "count"),
        ).reset_index()

        rain_stats["avg_vehicles"] = rain_stats["avg_vehicles"].round(1)
        rain_stats["avg_speed"] = rain_stats["avg_speed"].round(1)

        correlation = df["rain_factor"].corr(df["vehicle_count"])

        return {
            "correlation_coefficient": round(correlation, 3),
            "stats_by_category": rain_stats,
            "interpretation": self._interpret_correlation(correlation),
        }

    def _interpret_correlation(self, corr: float) -> str:
        if corr >= 0.7:
            return "Korelasi Kuat Positif: Hujan sangat mempengaruhi kemacetan"
        elif corr >= 0.4:
            return "Korelasi Sedang Positif: Hujan cukup mempengaruhi kemacetan"
        elif corr >= 0.2:
            return "Korelasi Lemah Positif: Hujan sedikit mempengaruhi kemacetan"
        else:
            return "Korelasi Sangat Lemah: Hujan tidak terlalu mempengaruhi"

    def get_location_comparison(self) -> pd.DataFrame:
        df = self.db.get_all_traffic_data()

        if df.empty:
            return pd.DataFrame()

        comparison = df.groupby("location").agg(
            avg_vehicles=("vehicle_count", "mean"),
            max_vehicles=("vehicle_count", "max"),
            min_vehicles=("vehicle_count", "min"),
            avg_speed=("speed_kmh", "mean"),
            total_records=("id", "count"),
            macet_count=("condition", lambda x: (x == "Macet").sum()),
        ).reset_index()

        comparison["avg_vehicles"] = comparison["avg_vehicles"].round(1)
        comparison["avg_speed"] = comparison["avg_speed"].round(1)

        comparison["macet_pct"] = (
            comparison["macet_count"] / comparison["total_records"] * 100
        ).round(1)

        return comparison.sort_values("avg_vehicles", ascending=False)

    def predict_traffic(self, location: str, target_hour: int) -> dict:
        df = self.db.get_traffic_by_location(location)

        if df.empty:
            return {"error": "Tidak ada data historis"}

        df_hour = df[df["hour"] == target_hour]

        if df_hour.empty:
            return {"error": f"Tidak ada data untuk jam {target_hour}:00"}

        avg_vehicles = df_hour["vehicle_count"].mean()
        std_vehicles = df_hour["vehicle_count"].std()
        avg_speed = df_hour["speed_kmh"].mean()

        predicted_min = max(0, int(avg_vehicles - std_vehicles))
        predicted_max = int(avg_vehicles + std_vehicles)
        predicted_avg = int(avg_vehicles)

        from config import TRAFFIC_THRESHOLDS
        condition = "Lancar"
        for cond, (low, high) in TRAFFIC_THRESHOLDS.items():
            if low <= predicted_avg < high:
                condition = cond
                break

        return {
            "location": location,
            "target_hour": target_hour,
            "predicted_vehicles_min": predicted_min,
            "predicted_vehicles_avg": predicted_avg,
            "predicted_vehicles_max": predicted_max,
            "predicted_speed": round(avg_speed, 1),
            "predicted_condition": condition,
            "confidence": "Sedang (berbasis rata-rata historis)",
            "samples_used": len(df_hour),
        }

    def get_weekday_vs_weekend(self) -> dict:
        df = self.db.get_all_traffic_data()

        if df.empty:
            return {"error": "Tidak ada data"}

        df["datetime"] = pd.to_datetime(df["timestamp"])
        df["day_of_week"] = df["datetime"].dt.dayofweek

        weekday = df[df["day_of_week"] < 5]
        weekend = df[df["day_of_week"] >= 5]

        result = {
            "weekday": {
                "label": "Hari Kerja (Sen-Jum)",
                "avg_vehicles": round(weekday["vehicle_count"].mean(), 1) if not weekday.empty else 0,
                "avg_speed": round(weekday["speed_kmh"].mean(), 1) if not weekday.empty else 0,
                "total_records": len(weekday),
            },
            "weekend": {
                "label": "Weekend (Sab-Min)",
                "avg_vehicles": round(weekend["vehicle_count"].mean(), 1) if not weekend.empty else 0,
                "avg_speed": round(weekend["speed_kmh"].mean(), 1) if not weekend.empty else 0,
                "total_records": len(weekend),
            },
        }

        return result

    def get_top_congestion(self, top_n: int = 10) -> pd.DataFrame:
        df = self.db.get_all_traffic_data()

        if df.empty:
            return pd.DataFrame()

        top = df.nlargest(top_n, "vehicle_count")[
            ["timestamp", "location", "vehicle_count", "condition", "speed_kmh", "rain_factor"]
        ]

        return top.reset_index(drop=True)

    def get_current_status(self) -> pd.DataFrame:
        df = self.db.get_all_traffic_data()

        if df.empty:
            return pd.DataFrame()

        df["datetime"] = pd.to_datetime(df["timestamp"])

        latest = df.sort_values("datetime").groupby("location").last().reset_index()

        return latest[["location", "vehicle_count", "condition", "speed_kmh", "rain_factor", "timestamp"]]