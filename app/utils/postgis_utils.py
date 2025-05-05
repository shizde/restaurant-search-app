from psycopg2.extras import RealDictCursor
from app.utils.db_utils import get_db_connection, execute_query


def initialize_postgis_indexes():
    """Enable PostGIS extension and create spatial indexes."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Enable PostGIS extension
        cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")

        # Check if geom column exists
        cursor.execute("""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'restaurants' AND column_name = 'geom'
        """)
        geom_exists = cursor.fetchone() is not None

        if not geom_exists:
            # Add geometry column
            cursor.execute("""
            ALTER TABLE restaurants 
            ADD COLUMN geom geometry(Point, 4326);
            """)

        # Update the geometry from lat/lng - using proper capitalized column names
        cursor.execute("""
        UPDATE restaurants
        SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326)
        WHERE geom IS NULL;
        """)

        # Create spatial index
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_restaurants_geom 
        ON restaurants USING GIST (geom);
        """)

        conn.commit()
        print("PostGIS extension and spatial indexes created successfully")
    except Exception as e:
        conn.rollback()
        print(f"Error initializing PostGIS: {e}")
    finally:
        cursor.close()
        conn.close()


def find_nearby_restaurants_postgis(lat, lng, radius_km):
    """
    Find restaurants using PostGIS spatial index.

    Args:
        lat (float): Latitude of center point
        lng (float): Longitude of center point
        radius_km (float): Search radius in kilometers

    Returns:
        list: Restaurants within the radius, ordered by distance
    """
    query = """
    SELECT 
        *, 
        ST_Distance(
            geom::geography, 
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
        ) / 1000 AS distance
    FROM 
        restaurants
    WHERE 
        ST_DWithin(
            geom::geography, 
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, 
            %s * 1000
        )
    ORDER BY 
        distance;
    """

    # Note: PostGIS uses (longitude, latitude) order in ST_MakePoint
    params = (lng, lat, lng, lat, radius_km)

    return execute_query(query, params)
