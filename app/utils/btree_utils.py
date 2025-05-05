import math
from psycopg2.extras import RealDictCursor
from app.utils.db_utils import get_db_connection, execute_query


def initialize_btree_indexes():
    """Create B-tree indexes on geographic columns for restaurants."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Create B-tree indexes on latitude and longitude
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_restaurants_latitude 
        ON restaurants USING btree (latitude);
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_restaurants_longitude 
        ON restaurants USING btree (longitude);
        """)

        # Create a combined index on both columns
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_restaurants_lat_lng 
        ON restaurants USING btree (latitude, longitude);
        """)

        # Create indexes on other commonly queried columns
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_restaurants_cuisine 
        ON restaurants USING btree (cuisine);
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_restaurants_price 
        ON restaurants USING btree (price);
        """)

        conn.commit()
        print("B-tree indexes created successfully")
    except Exception as e:
        conn.rollback()
        print(f"Error creating B-tree indexes: {e}")
    finally:
        cursor.close()
        conn.close()


def find_nearby_restaurants_btree(lat, lng, radius_km):
    """
    Find restaurants using B-tree indexes by first filtering with a bounding box.

    Args:
        lat (float): Latitude of center point
        lng (float): Longitude of center point
        radius_km (float): Search radius in kilometers

    Returns:
        list: Restaurants within the radius, ordered by distance
    """
    # Calculate approximate bounding box
    # 1 degree of latitude is approximately 111km
    lat_range = radius_km / 111.0

    # 1 degree of longitude varies with latitude
    # At the equator it's about 111km, decreasing with increasing latitude
    lng_range = radius_km / (111.0 * abs(math.cos(math.radians(lat))))

    # Query with pre-filtering using B-tree indexes
    query = """
    SELECT *, 
        (6371 * acos(cos(radians(%s)) * cos(radians(latitude)) * cos(radians(longitude) - 
        radians(%s)) + sin(radians(%s)) * sin(radians(latitude)))) AS distance 
    FROM restaurants 
    WHERE 
        latitude BETWEEN %s AND %s
        AND longitude BETWEEN %s AND %s
        AND (6371 * acos(cos(radians(%s)) * cos(radians(latitude)) * cos(radians(longitude) - 
            radians(%s)) + sin(radians(%s)) * sin(radians(latitude)))) < %s 
    ORDER BY distance;
    """

    params = (
        lat,
        lng,
        lat,
        lat - lat_range,
        lat + lat_range,
        lng - lng_range,
        lng + lng_range,
        lat,
        lng,
        lat,
        radius_km,
    )

    return execute_query(query, params)
