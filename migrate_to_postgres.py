
#!/usr/bin/env python3
"""
Migration script to populate PostgreSQL database with data from data.json
"""
import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from app.datastore import load_data


def create_tables(conn):
    """Create the database schema"""
    with conn.cursor() as cur:
        # Create seasons table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS seasons (
                id SERIAL PRIMARY KEY,
                year INTEGER UNIQUE NOT NULL
            )
        """)
        
        # Create series table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS series (
                id SERIAL PRIMARY KEY,
                series_id VARCHAR(100) UNIQUE NOT NULL,
                name VARCHAR(100) NOT NULL,
                season_id INTEGER REFERENCES seasons(id) ON DELETE CASCADE,
                year INTEGER NOT NULL
            )
        """)
        
        # Create races table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS races (
                id SERIAL PRIMARY KEY,
                race_id VARCHAR(200) UNIQUE NOT NULL,
                series_id VARCHAR(100) REFERENCES series(series_id) ON DELETE CASCADE,
                name VARCHAR(200),
                date DATE,
                start_time TIME,
                race_no INTEGER
            )
        """)
        
        # Create competitors table (fleet)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS competitors (
                id SERIAL PRIMARY KEY,
                competitor_id VARCHAR(20) UNIQUE NOT NULL,
                sailor_name VARCHAR(100),
                boat_name VARCHAR(100),
                sail_no VARCHAR(20),
                starting_handicap_s_per_hr INTEGER,
                current_handicap_s_per_hr INTEGER
            )
        """)
        
        # Create race_results table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS race_results (
                id SERIAL PRIMARY KEY,
                race_id VARCHAR(200) REFERENCES races(race_id) ON DELETE CASCADE,
                competitor_id VARCHAR(20) REFERENCES competitors(competitor_id) ON DELETE CASCADE,
                initial_handicap INTEGER,
                finish_time TIME,
                UNIQUE(race_id, competitor_id)
            )
        """)
        
        # Create settings table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                id SERIAL PRIMARY KEY,
                version INTEGER,
                updated_at TIMESTAMP,
                handicap_delta_by_rank JSONB,
                league_points_by_rank JSONB,
                fleet_size_factor JSONB,
                config JSONB
            )
        """)
        
        conn.commit()
        print("Database schema created successfully")


def migrate_fleet(conn, data):
    """Migrate fleet data to competitors table"""
    fleet = data.get('fleet', {})
    competitors = fleet.get('competitors', [])
    
    with conn.cursor() as cur:
        # Clear existing data
        cur.execute("DELETE FROM competitors")
        
        for comp in competitors:
            # Generate competitor_id from sail_no if not present
            sail_no = comp.get('sail_no', '')
            competitor_id = f"C_{sail_no}" if sail_no else None
            
            if competitor_id:
                cur.execute("""
                    INSERT INTO competitors (
                        competitor_id, sailor_name, boat_name, sail_no,
                        starting_handicap_s_per_hr, current_handicap_s_per_hr
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (competitor_id) DO UPDATE SET
                        sailor_name = EXCLUDED.sailor_name,
                        boat_name = EXCLUDED.boat_name,
                        sail_no = EXCLUDED.sail_no,
                        starting_handicap_s_per_hr = EXCLUDED.starting_handicap_s_per_hr,
                        current_handicap_s_per_hr = EXCLUDED.current_handicap_s_per_hr
                """, (
                    competitor_id,
                    comp.get('sailor_name'),
                    comp.get('boat_name'),
                    comp.get('sail_no'),
                    comp.get('starting_handicap_s_per_hr'),
                    comp.get('current_handicap_s_per_hr')
                ))
        
        conn.commit()
        print(f"Migrated {len(competitors)} competitors")


def migrate_seasons_and_series(conn, data):
    """Migrate seasons and series data"""
    seasons = data.get('seasons', [])
    
    with conn.cursor() as cur:
        # Clear existing data (cascades to series)
        cur.execute("DELETE FROM seasons")
        
        for season in seasons:
            year = season.get('year')
            
            # Insert season
            cur.execute("""
                INSERT INTO seasons (year) VALUES (%s)
                ON CONFLICT (year) DO NOTHING
            """, (year,))
            
            # Get season id
            cur.execute("SELECT id FROM seasons WHERE year = %s", (year,))
            season_id = cur.fetchone()[0]
            
            # Insert series
            for series in season.get('series', []):
                cur.execute("""
                    INSERT INTO series (series_id, name, season_id, year)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (series_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        season_id = EXCLUDED.season_id,
                        year = EXCLUDED.year
                """, (
                    series.get('series_id'),
                    series.get('name'),
                    season_id,
                    year
                ))
        
        conn.commit()
        print(f"Migrated {len(seasons)} seasons")


def migrate_races_and_results(conn, data):
    """Migrate races and race results"""
    seasons = data.get('seasons', [])
    
    with conn.cursor() as cur:
        # Clear existing race data
        cur.execute("DELETE FROM race_results")
        cur.execute("DELETE FROM races")
        
        for season in seasons:
            for series in season.get('series', []):
                for race in series.get('races', []):
                    # Insert race
                    date_str = race.get('date')
                    start_time_str = race.get('start_time')
                    
                    cur.execute("""
                        INSERT INTO races (
                            race_id, series_id, name, date, start_time, race_no
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (race_id) DO UPDATE SET
                            series_id = EXCLUDED.series_id,
                            name = EXCLUDED.name,
                            date = EXCLUDED.date,
                            start_time = EXCLUDED.start_time,
                            race_no = EXCLUDED.race_no
                    """, (
                        race.get('race_id'),
                        series.get('series_id'),
                        race.get('name'),
                        date_str,
                        start_time_str,
                        race.get('race_no')
                    ))
                    
                    # Insert race results
                    for competitor in race.get('competitors', []):
                        finish_time_str = competitor.get('finish_time')
                        
                        cur.execute("""
                            INSERT INTO race_results (
                                race_id, competitor_id, initial_handicap, finish_time
                            ) VALUES (%s, %s, %s, %s)
                            ON CONFLICT (race_id, competitor_id) DO UPDATE SET
                                initial_handicap = EXCLUDED.initial_handicap,
                                finish_time = EXCLUDED.finish_time
                        """, (
                            race.get('race_id'),
                            competitor.get('competitor_id'),
                            competitor.get('initial_handicap'),
                            finish_time_str
                        ))
        
        conn.commit()
        print("Migrated races and race results")


def migrate_settings(conn, data):
    """Migrate settings data"""
    settings = data.get('settings', {})
    
    with conn.cursor() as cur:
        # Clear existing settings
        cur.execute("DELETE FROM settings")
        
        # Insert settings with individual arrays and full config
        cur.execute("""
            INSERT INTO settings (
                version, updated_at, handicap_delta_by_rank, 
                league_points_by_rank, fleet_size_factor, config
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            settings.get('version'),
            settings.get('updated_at'),
            json.dumps(settings.get('handicap_delta_by_rank', [])),
            json.dumps(settings.get('league_points_by_rank', [])),
            json.dumps(settings.get('fleet_size_factor', [])),
            json.dumps(settings)
        ))
        
        conn.commit()
        print("Migrated settings")


def main():
    """Main migration function"""
    # Get database connection
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        print("ERROR: DATABASE_URL environment variable not set")
        print("Please create a PostgreSQL database in Replit first")
        return
    
    # Load data from JSON
    print("Loading data from data.json...")
    data = load_data()
    
    try:
        # Connect to database
        conn = psycopg2.connect(database_url)
        print("Connected to PostgreSQL database")
        
        # Create schema
        create_tables(conn)
        
        # Migrate data
        migrate_fleet(conn, data)
        migrate_seasons_and_series(conn, data)
        migrate_races_and_results(conn, data)
        migrate_settings(conn, data)
        
        print("\nMigration completed successfully!")
        
        # Show summary
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM competitors")
            comp_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM seasons")
            season_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM series")
            series_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM races")
            race_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM race_results")
            result_count = cur.fetchone()[0]
            
            print(f"\nSummary:")
            print(f"- {comp_count} competitors")
            print(f"- {season_count} seasons")
            print(f"- {series_count} series")
            print(f"- {race_count} races")
            print(f"- {result_count} race results")
        
    except Exception as e:
        print(f"Error during migration: {e}")
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    main()
