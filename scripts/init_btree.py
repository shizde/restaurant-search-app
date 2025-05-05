#!/usr/bin/env python3
import os
from app.utils.db_utils import get_db_connection
from scripts.init_basic import init_basic_db, get_csv_column_names


def initialize_btree_indexes():
    """Create B-tree indexes on geographic columns for restaurants."""
    print("Creating B-tree indexes...")

    # Get CSV column names to determine actual column names
    restaurants_csv_path = "/app/data/Restaurants.csv"
    restaurant_columns = get_csv_column_names(restaurants_csv_path)

    if not restaurant_columns:
        print("Error: Could not read restaurant CSV headers. Aborting index creation.")
        return

    # Find latitude and longitude columns
    latitude_col = None
    longitude_col = None
    cuisine_col = None
    price_col = None

    for col in restaurant_columns:
        if col.lower() == "latitude":
            latitude_col = col
        elif col.lower() == "longitude":
            longitude_col = col
        elif "cuisine" in col.lower():
            cuisine_col = col
        elif "price" in col.lower():
            price_col = col

    if not latitude_col or not longitude_col:
        print("Error: Could not find latitude or longitude columns")
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Create B-tree indexes on latitude and longitude with quoted column names
        cursor.execute(f'''
        CREATE INDEX IF NOT EXISTS idx_restaurants_latitude 
        ON restaurants USING btree ("{latitude_col}");
        ''')

        cursor.execute(f'''
        CREATE INDEX IF NOT EXISTS idx_restaurants_longitude 
        ON restaurants USING btree ("{longitude_col}");
        ''')

        # Create a combined index on both columns
        cursor.execute(f'''
        CREATE INDEX IF NOT EXISTS idx_restaurants_lat_lng 
        ON restaurants USING btree ("{latitude_col}", "{longitude_col}");
        ''')

        # Create indexes on other commonly queried columns if they exist
        if cuisine_col:
            cursor.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_restaurants_cuisine 
            ON restaurants USING btree ("{cuisine_col}");
            ''')

        if price_col:
            cursor.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_restaurants_price 
            ON restaurants USING btree ("{price_col}");
            ''')

        conn.commit()
        print("B-tree indexes created successfully")
    except Exception as e:
        conn.rollback()
        print(f"Error creating B-tree indexes: {e}")
    finally:
        cursor.close()
        conn.close()


def init_btree_db():
    """Initialize database with B-tree indexes."""
    # First initialize with basic setup
    init_basic_db()

    # Then create B-tree indexes
    initialize_btree_indexes()

    print("B-tree database initialization completed")


if __name__ == "__main__":
    init_btree_db()
