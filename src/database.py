import sqlite3
import pandas as pd
from config import DATABASE_PATH


class TrafficDatabase:
    def __init__(self):
        self.db_path = DATABASE_PATH
        print(f"📁 Database path: {self.db_path}")

    def get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def init_tables(self):
        print("🗄️  Initializing database tables...")
        
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS traffic_data (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT NOT NULL,
                location    TEXT NOT NULL,
                vehicle_count INTEGER NOT NULL,
                condition   TEXT NOT NULL,
                speed_kmh   REAL,
                hour        INTEGER,
                is_peak     INTEGER DEFAULT 0,
                rain_factor REAL DEFAULT 1.0,
                data_source TEXT DEFAULT 'simulated'
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp       TEXT NOT NULL,
                location        TEXT NOT NULL,
                temperature     REAL,
                precipitation   REAL,
                windspeed       REAL,
                weather_code    INTEGER,
                weather_desc    TEXT,
                rain_category   TEXT DEFAULT 'none'
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS traffic_analysis (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_date   TEXT NOT NULL,
                location        TEXT NOT NULL,
                avg_vehicles    REAL,
                max_vehicles    INTEGER,
                min_vehicles    INTEGER,
                avg_speed       REAL,
                peak_hour       INTEGER,
                rain_correlation REAL,
                total_records   INTEGER
            )
        """)

        conn.commit()
        conn.close()
        print("✅ Tables created successfully!")

    def insert_traffic_data(self, records: list):
        if not records:
            return

        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.executemany("""
            INSERT INTO traffic_data 
            (timestamp, location, vehicle_count, condition, speed_kmh, 
             hour, is_peak, rain_factor, data_source)
            VALUES 
            (:timestamp, :location, :vehicle_count, :condition, :speed_kmh,
             :hour, :is_peak, :rain_factor, :data_source)
        """, records)

        conn.commit()
        conn.close()
        print(f"✅ Inserted {len(records)} traffic records")

    def insert_weather_data(self, record: dict):
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO weather_data 
            (timestamp, location, temperature, precipitation, windspeed,
             weather_code, weather_desc, rain_category)
            VALUES 
            (:timestamp, :location, :temperature, :precipitation, :windspeed,
             :weather_code, :weather_desc, :rain_category)
        """, record)

        conn.commit()
        conn.close()

    def insert_analysis(self, record: dict):
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO traffic_analysis 
            (analysis_date, location, avg_vehicles, max_vehicles, min_vehicles,
             avg_speed, peak_hour, rain_correlation, total_records)
            VALUES 
            (:analysis_date, :location, :avg_vehicles, :max_vehicles, :min_vehicles,
             :avg_speed, :peak_hour, :rain_correlation, :total_records)
        """, record)

        conn.commit()
        conn.close()

    def get_all_traffic_data(self) -> pd.DataFrame:
        conn = self.get_connection()
        df = pd.read_sql_query("SELECT * FROM traffic_data ORDER BY timestamp DESC", conn)
        conn.close()
        return df

    def get_traffic_by_location(self, location: str) -> pd.DataFrame:
        conn = self.get_connection()
        df = pd.read_sql_query(
            "SELECT * FROM traffic_data WHERE location = ? ORDER BY timestamp DESC",
            conn,
            params=(location,)
        )
        conn.close()
        return df

    def get_traffic_by_date_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        conn = self.get_connection()
        df = pd.read_sql_query(
            "SELECT * FROM traffic_data WHERE timestamp BETWEEN ? AND ? ORDER BY timestamp",
            conn,
            params=(start_date, end_date)
        )
        conn.close()
        return df

    def get_latest_traffic(self, limit: int = 10) -> pd.DataFrame:
        conn = self.get_connection()
        df = pd.read_sql_query(
            "SELECT * FROM traffic_data ORDER BY timestamp DESC LIMIT ?",
            conn,
            params=(limit,)
        )
        conn.close()
        return df

    def get_all_weather_data(self) -> pd.DataFrame:
        conn = self.get_connection()
        df = pd.read_sql_query("SELECT * FROM weather_data ORDER BY timestamp DESC", conn)
        conn.close()
        return df

    def get_latest_weather(self) -> pd.DataFrame:
        conn = self.get_connection()
        df = pd.read_sql_query("""
            SELECT * FROM weather_data 
            WHERE id IN (
                SELECT MAX(id) FROM weather_data GROUP BY location
            )
            ORDER BY location
        """, conn)
        conn.close()
        return df

    def get_traffic_count(self) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as total FROM traffic_data")
        result = cursor.fetchone()
        conn.close()
        return result["total"]

    def get_weather_count(self) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as total FROM weather_data")
        result = cursor.fetchone()
        conn.close()
        return result["total"]

    def get_hourly_avg(self, location: str = None) -> pd.DataFrame:
        conn = self.get_connection()
        query = """
            SELECT 
                hour,
                AVG(vehicle_count) as avg_vehicles,
                AVG(speed_kmh) as avg_speed,
                COUNT(*) as total_records
            FROM traffic_data
        """
        if location:
            query += " WHERE location = ?"
            df = pd.read_sql_query(query + " GROUP BY hour ORDER BY hour", conn, params=(location,))
        else:
            df = pd.read_sql_query(query + " GROUP BY hour ORDER BY hour", conn)
        
        conn.close()
        return df

    def clear_all_data(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM traffic_data")
        cursor.execute("DELETE FROM weather_data")
        cursor.execute("DELETE FROM traffic_analysis")
        conn.commit()
        conn.close()
        print("🗑️  All data cleared!")