import psycopg2
import time
import os
import sys

# Configuration from environment or defaults
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_NAME = os.getenv("POSTGRES_DB", "weather_db")
DB_USER = os.getenv("POSTGRES_USER", "sensorfact")
DB_PASS = os.getenv("POSTGRES_PASS", "password")

def get_latest_forecasts():
    """
    Queries the database for the most recent precipitation forecast for every location.
    Uses DISTINCT ON to efficiently get exactly one row per location_name.
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            connect_timeout=5
        )
        cur = conn.cursor()

        # Optimized query for real-time state
        query = """
        SELECT DISTINCT ON (location_name)
            location_name,
            total_next_hour_mm,
            forecast_window_start,
            lat,
            lon
        FROM precipitation_forecasts
        ORDER BY location_name, forecast_window_start DESC;
        """

        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        return f"Database Error: {e}"

def clear_console():
    """Clears the terminal screen based on the OS."""
    os.system('cls' if os.name == 'nt' else 'clear')

if __name__ == "__main__":
    try:
        while True:
            results = get_latest_forecasts()
            clear_console()

            print("=" * 70)
            print(f"{'SENSORFACT WEATHER MONITOR (LIVE)':^70}")
            print("=" * 70)
            print(f"{'Location':<25} | {'Rain (mm)':<10} | {'Forecast Start (CET)':<20}")
            print("-" * 70)

            if isinstance(results, str):
                print(f"\n[!] {results}")
            elif not results:
                print("\n[?] No data found yet. Waiting for Spark to process...")
            else:
                for row in results:
                    name, rain, start, lat, lon = row
                    # Format timestamp to string for clean display
                    time_str = start.strftime("%H:%M:%S") if hasattr(start, 'strftime') else str(start)
                    print(f"{name:<25} | {rain:>9.2f} | {time_str:<20}")

            print("-" * 70)
            print(f"Last updated: {time.strftime('%H:%M:%S')} | Refreshing in 10s...")
            time.sleep(10)

    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")
        sys.exit(0)