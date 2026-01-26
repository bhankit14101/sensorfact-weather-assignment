import os
import time
import psycopg2
from datetime import datetime

# Database connection details (matching docker-compose)
DB_CONFIG = {
    "dbname": "weather_db",
    "user": "sensorfact",
    "password": "password",
    "host": "localhost",
    "port": "5432"
}

def monitor():
    print("Starting Live Weather Monitor...")
    print("Press Ctrl+C to stop.\n")

    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()

            # Query the latest results
            cur.execute("""
                SELECT location_name, total_next_hour_mm, lat, lon, forecast_window_start 
                FROM precipitation_forecasts 
                ORDER BY location_name;
            """)
            rows = cur.fetchall()

            # Clear terminal (works on most systems)
            os.system('cls' if os.name == 'nt' else 'clear')

            print(f"--- SENSORFACT LIVE PRECIPITATION MONITOR ---")
            print(f"Last Updated: {datetime.now().strftime('%H:%M:%S')}")
            print("-" * 65)
            print(f"{'Location':<15} | {'Total (mm)':<12} | {'Coordinates':<18} | {'Forecast Start'}")
            print("-" * 65)

            for row in rows:
                loc, val, lat, lon, start = row
                coord = f"{lat}, {lon}"
                print(f"{loc:<15} | {val:<12.2f} | {coord:<18} | {start}")

            cur.close()
            conn.close()

        except Exception as e:
            print(f"Waiting for database connection... ({e})")

        time.sleep(5) # Refresh every 5 seconds

if __name__ == "__main__":
    monitor()