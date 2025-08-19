import os
import sqlite3
import pandas as pd
from dotenv import load_dotenv
import requests
import json
from datetime import datetime, timedelta
import logging
import time
import threading
import signal
import sys
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('realtime_pipeline.log'),
        logging.StreamHandler()
    ]
)

class RealTimeDublinBusPipeline:
    def __init__(self, scrape_interval=120):  # Default 2 minutes
        """
        Initialize the real-time pipeline
        
        Args:
            scrape_interval (int): Seconds between API calls (default 120 = 2 minutes)
        """
        load_dotenv()
        self.api_key = os.getenv("X_API_KEY")
        self.base_url = "https://api.nationaltransport.ie/gtfsr/v2/gtfsr?format=json"
        self.scrape_interval = scrape_interval
        self.running = False
        
        # Ensure directories exist
        Path("data_exports").mkdir(exist_ok=True)
        Path("logs").mkdir(exist_ok=True)
        
        # Initialize database
        self.init_database()
        
        # Set up graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logging.info("Shutdown signal received. Stopping pipeline...")
        self.running = False
        
    def init_database(self):
        """Initialize SQLite database with optimized schema for real-time use"""
        self.conn = sqlite3.connect('dublin_bus_realtime.db', check_same_thread=False)
        
        # Register datetime adapters to fix Python 3.12 deprecation warnings
        def adapt_datetime(dt):
            return dt.isoformat()
        
        def convert_datetime(s):
            return datetime.fromisoformat(s.decode())
        
        sqlite3.register_adapter(datetime, adapt_datetime)
        sqlite3.register_converter("DATETIME", convert_datetime)
        
        cursor = self.conn.cursor()
        
        # Enable WAL mode for better concurrent access
        cursor.execute("PRAGMA journal_mode=WAL;")
        
        # Create trips table with indexes for performance
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trip_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fetch_timestamp TEXT DEFAULT (datetime('now')),
                gtfs_timestamp INTEGER,
                entity_id TEXT,
                trip_id TEXT,
                route_id TEXT,
                start_time TEXT,
                start_date TEXT,
                direction_id INTEGER,
                schedule_relationship TEXT,
                vehicle_id TEXT,
                vehicle_label TEXT,
                trip_timestamp INTEGER,
                -- Add calculated fields for easier analysis
                fetch_time_local TEXT,
                hour_of_day INTEGER,
                day_of_week TEXT,
                is_weekend INTEGER
            )
        ''')
        
        # Create indexes for faster queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trip_route_time ON trip_updates(route_id, fetch_timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trip_entity ON trip_updates(entity_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trip_fetch_time ON trip_updates(fetch_timestamp)')
        
        # Create stop time updates table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stop_time_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trip_update_id INTEGER,
                entity_id TEXT,
                trip_id TEXT,
                route_id TEXT,
                stop_sequence INTEGER,
                stop_id TEXT,
                arrival_delay INTEGER,
                arrival_time INTEGER,
                departure_delay INTEGER,
                departure_time INTEGER,
                schedule_relationship TEXT,
                fetch_timestamp TEXT DEFAULT (datetime('now')),
                -- Calculated fields
                delay_minutes REAL,
                delay_status TEXT,
                is_delayed INTEGER,
                FOREIGN KEY (trip_update_id) REFERENCES trip_updates (id)
            )
        ''')
        
        # Create indexes for stop updates
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_stop_route_time ON stop_time_updates(route_id, fetch_timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_stop_delay ON stop_time_updates(arrival_delay)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_stop_entity ON stop_time_updates(entity_id)')
        
        # Create real-time summary table for Tableau
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS realtime_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                update_timestamp TEXT DEFAULT (datetime('now')),
                route_id TEXT,
                total_trips INTEGER,
                avg_delay_minutes REAL,
                on_time_percentage REAL,
                delayed_trips INTEGER,
                early_trips INTEGER,
                max_delay_minutes REAL,
                direction_id INTEGER
            )
        ''')
        
        self.conn.commit()
        logging.info("Database initialized with real-time optimizations")
        
    def fetch_realtime_data(self):
        """Fetch data from Dublin Bus API with retry logic"""
        headers = {"x-api-key": self.api_key}
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                response = requests.get(self.base_url, headers=headers, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                entities_count = len(data.get('entity', []))
                logging.info(f"Successfully fetched {entities_count} entities (attempt {attempt + 1})")
                return data
                
            except requests.exceptions.RequestException as e:
                logging.warning(f"API request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(10)  # Wait 10 seconds before retry
                else:
                    logging.error("Max retries exceeded")
                    return None
            except json.JSONDecodeError as e:
                logging.error(f"JSON decode error: {e}")
                return None
                
    def process_trip_updates(self, data):
        """Process and store trip update data with real-time calculations"""
        if not data or 'entity' not in data:
            logging.warning("No entity data found")
            return 0
            
        header = data.get('header', {})
        gtfs_timestamp = header.get('timestamp')
        fetch_time = datetime.now()
        
        cursor = self.conn.cursor()
        processed_count = 0
        
        for entity in data['entity']:
            if 'trip_update' not in entity:
                continue
                
            entity_id = entity.get('id')
            trip_update = entity['trip_update']
            trip_info = trip_update.get('trip', {})
            vehicle_info = trip_update.get('vehicle', {})
            
            # Calculate time-based fields
            hour_of_day = fetch_time.hour
            day_of_week = fetch_time.strftime('%A')
            is_weekend = 1 if fetch_time.weekday() >= 5 else 0
            
            # Insert trip update with calculated fields
            cursor.execute('''
                INSERT INTO trip_updates 
                (gtfs_timestamp, entity_id, trip_id, route_id, start_time, start_date, 
                 direction_id, schedule_relationship, vehicle_id, vehicle_label, trip_timestamp,
                 fetch_time_local, hour_of_day, day_of_week, is_weekend)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                gtfs_timestamp, entity_id, trip_info.get('trip_id'), trip_info.get('route_id'),
                trip_info.get('start_time'), trip_info.get('start_date'), trip_info.get('direction_id'),
                trip_info.get('schedule_relationship'), vehicle_info.get('id'), vehicle_info.get('label'),
                trip_update.get('timestamp'), fetch_time.isoformat(), hour_of_day, day_of_week, is_weekend
            ))
            
            trip_update_id = cursor.lastrowid
            
            # Process stop time updates with delay calculations
            for stop_update in trip_update.get('stop_time_update', []):
                arrival = stop_update.get('arrival', {})
                departure = stop_update.get('departure', {})
                arrival_delay = arrival.get('delay')
                
                # Calculate delay metrics
                delay_minutes = arrival_delay / 60.0 if arrival_delay is not None else None
                delay_status = 'Unknown'
                is_delayed = 0
                
                if arrival_delay is not None:
                    if arrival_delay > 300:  # More than 5 minutes late
                        delay_status = 'Significantly Late'
                        is_delayed = 1
                    elif arrival_delay > 60:  # More than 1 minute late
                        delay_status = 'Late'
                        is_delayed = 1
                    elif arrival_delay < -60:  # More than 1 minute early
                        delay_status = 'Early'
                    else:
                        delay_status = 'On Time'
                
                cursor.execute('''
                    INSERT INTO stop_time_updates 
                    (trip_update_id, entity_id, trip_id, route_id, stop_sequence, stop_id,
                     arrival_delay, arrival_time, departure_delay, departure_time, 
                     schedule_relationship, delay_minutes, delay_status, is_delayed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    trip_update_id, entity_id, trip_info.get('trip_id'), trip_info.get('route_id'),
                    stop_update.get('stop_sequence'), stop_update.get('stop_id'),
                    arrival_delay, arrival.get('time'), departure.get('delay'), departure.get('time'),
                    stop_update.get('schedule_relationship'), delay_minutes, delay_status, is_delayed
                ))
            
            processed_count += 1
        
        self.conn.commit()
        logging.info(f"Processed {processed_count} trip updates")
        return processed_count
        
    def update_realtime_summary(self):
        """Update summary table for fast Tableau queries"""
        cursor = self.conn.cursor()
        
        # Calculate summary statistics by route and direction
        cursor.execute('''
            INSERT INTO realtime_summary 
            (route_id, total_trips, avg_delay_minutes, on_time_percentage, 
             delayed_trips, early_trips, max_delay_minutes, direction_id)
            SELECT 
                stu.route_id,
                COUNT(DISTINCT stu.trip_id) as total_trips,
                AVG(stu.delay_minutes) as avg_delay_minutes,
                (SUM(CASE WHEN stu.delay_status = 'On Time' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as on_time_percentage,
                SUM(CASE WHEN stu.is_delayed = 1 THEN 1 ELSE 0 END) as delayed_trips,
                SUM(CASE WHEN stu.delay_status = 'Early' THEN 1 ELSE 0 END) as early_trips,
                MAX(stu.delay_minutes) as max_delay_minutes,
                tu.direction_id
            FROM stop_time_updates stu
            JOIN trip_updates tu ON stu.entity_id = tu.entity_id
            WHERE stu.fetch_timestamp >= datetime('now', '-10 minutes')
            AND stu.arrival_delay IS NOT NULL
            GROUP BY stu.route_id, tu.direction_id
        ''')
        
        self.conn.commit()
        
    def export_realtime_data(self):
        """Export current data for Tableau with different time windows"""
        current_time = datetime.now()
        two_hours_ago = (current_time - timedelta(hours=2)).isoformat()
        one_hour_ago = (current_time - timedelta(hours=1)).isoformat()
        twenty_four_hours_ago = (current_time - timedelta(hours=24)).isoformat()
        
        # Export last 2 hours of data (for real-time dashboard)
        query_recent = '''
            SELECT 
                tu.fetch_timestamp,
                tu.entity_id,
                tu.trip_id,
                tu.route_id,
                tu.direction_id,
                tu.start_time,
                tu.vehicle_id,
                tu.hour_of_day,
                tu.day_of_week,
                tu.is_weekend,
                stu.stop_id,
                stu.stop_sequence,
                stu.delay_minutes,
                stu.delay_status,
                stu.is_delayed,
                CASE tu.direction_id 
                    WHEN 0 THEN 'Inbound' 
                    WHEN 1 THEN 'Outbound' 
                    ELSE 'Unknown' 
                END as direction_name,
                substr(tu.fetch_timestamp, 12, 5) as time_only,
                substr(tu.fetch_timestamp, 1, 10) as date_only
            FROM trip_updates tu
            LEFT JOIN stop_time_updates stu ON tu.id = stu.trip_update_id
            WHERE tu.fetch_timestamp >= ?
            ORDER BY tu.fetch_timestamp DESC, tu.route_id, stu.stop_sequence
        '''
        
        df_recent = pd.read_sql_query(query_recent, self.conn, params=[two_hours_ago])
        df_recent.to_csv('data_exports/dublin_bus_realtime.csv', index=False)
        
        # Export summary data
        query_summary = '''
            SELECT * FROM realtime_summary 
            WHERE update_timestamp >= ?
            ORDER BY update_timestamp DESC
        '''
        
        df_summary = pd.read_sql_query(query_summary, self.conn, params=[one_hour_ago])
        df_summary.to_csv('data_exports/dublin_bus_summary.csv', index=False)
        
        # Export route performance (last 24 hours)
        query_performance = '''
            SELECT 
                stu.route_id,
                tu.direction_id,
                COUNT(*) as total_observations,
                AVG(stu.delay_minutes) as avg_delay,
                MIN(stu.delay_minutes) as min_delay,
                MAX(stu.delay_minutes) as max_delay,
                SUM(CASE WHEN stu.delay_status = 'On Time' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as on_time_pct,
                SUM(stu.is_delayed) as delayed_count,
                tu.hour_of_day
            FROM stop_time_updates stu
            JOIN trip_updates tu ON stu.entity_id = tu.entity_id
            WHERE stu.fetch_timestamp >= ?
            AND stu.arrival_delay IS NOT NULL
            GROUP BY stu.route_id, tu.direction_id, tu.hour_of_day
            ORDER BY stu.route_id, tu.direction_id, tu.hour_of_day
        '''
        
        df_performance = pd.read_sql_query(query_performance, self.conn, params=[twenty_four_hours_ago])
        df_performance.to_csv('data_exports/dublin_bus_performance.csv', index=False)
        
        logging.info(f"Exported {len(df_recent)} recent records, {len(df_summary)} summary records, {len(df_performance)} performance records")
        
    def cleanup_old_data(self, days_to_keep=7):
        """Clean up old data to manage database size"""
        cutoff_time = datetime.now() - timedelta(days=days_to_keep)
        cursor = self.conn.cursor()
        
        try:
            # Delete old records
            cursor.execute("DELETE FROM stop_time_updates WHERE fetch_timestamp < ?", (cutoff_time.isoformat(),))
            deleted_stops = cursor.rowcount
            
            cursor.execute("DELETE FROM trip_updates WHERE fetch_timestamp < ?", (cutoff_time.isoformat(),))
            deleted_trips = cursor.rowcount
            
            cursor.execute("DELETE FROM realtime_summary WHERE update_timestamp < ?", (cutoff_time.isoformat(),))
            deleted_summary = cursor.rowcount
            
            # Commit the deletions first
            self.conn.commit()
            
            # Vacuum database to reclaim space (must be outside transaction)
            self.conn.execute("VACUUM")
            
            logging.info(f"Cleanup: Deleted {deleted_trips} trips, {deleted_stops} stops, {deleted_summary} summaries")
            
        except Exception as e:
            logging.error(f"Cleanup failed: {e}")
            self.conn.rollback()
        
    def get_pipeline_stats(self):
        """Get current pipeline statistics"""
        cursor = self.conn.cursor()
        
        # Get total records
        cursor.execute("SELECT COUNT(*) FROM trip_updates")
        total_trips = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM stop_time_updates")
        total_stops = cursor.fetchone()[0]
        
        # Get recent activity
        cursor.execute("""
            SELECT COUNT(*) FROM trip_updates 
            WHERE fetch_timestamp >= datetime('now', '-1 hour')
        """)
        recent_trips = cursor.fetchone()[0]
        
        # Get route coverage
        cursor.execute("SELECT COUNT(DISTINCT route_id) FROM trip_updates")
        unique_routes = cursor.fetchone()[0]
        
        # Get latest update
        cursor.execute("SELECT MAX(fetch_timestamp) FROM trip_updates")
        latest_update = cursor.fetchone()[0]
        
        return {
            'total_trips': total_trips,
            'total_stops': total_stops,
            'recent_trips_1h': recent_trips,
            'unique_routes': unique_routes,
            'latest_update': latest_update
        }
        
    def run_single_cycle(self):
        """Run a single data collection and processing cycle"""
        try:
            # Fetch data
            data = self.fetch_realtime_data()
            if not data:
                return False
            
            # Process data
            processed_count = self.process_trip_updates(data)
            
            # Update summary
            self.update_realtime_summary()
            
            # Export data for Tableau
            self.export_realtime_data()
            
            # Get stats
            stats = self.get_pipeline_stats()
            logging.info(f"Cycle complete. Stats: {stats}")
            
            return True
            
        except Exception as e:
            logging.error(f"Cycle failed: {e}")
            return False
            
    def run_realtime_pipeline(self):
        """Run the continuous real-time pipeline"""
        self.running = True
        logging.info(f"Starting real-time pipeline (interval: {self.scrape_interval}s)")
        
        # Run cleanup on startup
        self.cleanup_old_data()
        
        cycle_count = 0
        cleanup_interval = 24  # hours
        last_cleanup = datetime.now()
        
        while self.running:
            try:
                cycle_start = time.time()
                
                # Run data collection cycle
                success = self.run_single_cycle()
                
                cycle_count += 1
                cycle_duration = time.time() - cycle_start
                
                if success:
                    logging.info(f"Cycle {cycle_count} completed in {cycle_duration:.2f}s")
                else:
                    logging.warning(f"Cycle {cycle_count} failed")
                
                # Periodic cleanup
                if datetime.now() - last_cleanup > timedelta(hours=cleanup_interval):
                    self.cleanup_old_data()
                    last_cleanup = datetime.now()
                
                # Sleep until next cycle
                if self.running:
                    sleep_time = max(0, self.scrape_interval - cycle_duration)
                    time.sleep(sleep_time)
                    
            except Exception as e:
                logging.error(f"Pipeline error: {e}")
                time.sleep(60)  # Wait 1 minute on error
                
        logging.info("Real-time pipeline stopped")
        
    def close(self):
        """Close database connection"""
        if hasattr(self, 'conn'):
            self.conn.close()

def main():
    # Configuration
    SCRAPE_INTERVAL = 120  # 2 minutes between API calls
    
    pipeline = RealTimeDublinBusPipeline(scrape_interval=SCRAPE_INTERVAL)
    
    try:
        print("=== Dublin Bus Real-Time Pipeline ===")
        print(f"Scrape interval: {SCRAPE_INTERVAL} seconds")
        print("Starting pipeline... Press Ctrl+C to stop")
        
        # Run the real-time pipeline
        pipeline.run_realtime_pipeline()
        
    except KeyboardInterrupt:
        logging.info("Shutdown requested by user")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
    finally:
        pipeline.close()
        print("Pipeline stopped")

if __name__ == "__main__":
    main()