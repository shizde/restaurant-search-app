import h3
import math
from psycopg2.extras import RealDictCursor
from app.utils.db_utils import get_db_connection, execute_query


def get_h3_resolution_for_radius(radius_km):
    """
    Determine the appropriate H3 resolution for a given radius in kilometers.

    Args:
        radius_km (float): Radius in kilometers

    Returns:
        int: H3 resolution (0-15)
    """
    # Average hexagon edge lengths in km for each resolution
    avg_hex_edge_lengths = {
        0: 1107.712591,
        1: 418.6760055,
        2: 158.2446558,
        3: 59.81085794,
        4: 22.6063794,
        5: 8.544408276,
        6: 3.229482772,
        7: 1.220629759,
        8: 0.461354684,
        9: 0.174375668,
        10: 0.065907807,
        11: 0.024910561,
        12: 0.009415526,
        13: 0.003559893,
        14: 0.001348575,
        15: 0.000509713,
    }

    # Find the resolution where the average hexagon edge length is closest to the radius
    closest_res = min(
        avg_hex_edge_lengths.keys(),
        key=lambda res: abs(avg_hex_edge_lengths[res] - radius_km),
    )

    return min(15, closest_res + 1)  # Max resolution is 15


def initialize_h3_indexes():
    """Add H3 index columns and populate with H3 indexes."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Check if H3 columns exist
        cursor.execute("""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'restaurants' AND column_name = 'h3_index_res8'
        """)
        h3_exists = cursor.fetchone() is not None

        if not h3_exists:
            # Add H3 index columns
            cursor.execute("""
            ALTER TABLE restaurants 
            ADD COLUMN h3_index_res8 TEXT,
            ADD COLUMN h3_index_res9 TEXT,
            ADD COLUMN h3_index_res10 TEXT;
            """)

        # Update H3 indexes for rows where they are NULL
        cursor.execute("""
        SELECT "Restaurantid", "Latitude", "Longitude" 
        FROM restaurants 
        WHERE h3_index_res8 IS NULL OR h3_index_res9 IS NULL OR h3_index_res10 IS NULL
        """)
        restaurants = cursor.fetchall()

        for restaurant in restaurants:
            restaurant_id, lat, lng = restaurant

            h3_index_res8 = h3.geo_to_h3(lat, lng, 8)
            h3_index_res9 = h3.geo_to_h3(lat, lng, 9)
            h3_index_res10 = h3.geo_to_h3(lat, lng, 10)

            cursor.execute(
                """
            UPDATE restaurants
            SET h3_index_res8 = %s, h3_index_res9 = %s, h3_index_res10 = %s
            WHERE "Restaurantid" = %s
            """,
                (h3_index_res8, h3_index_res9, h3_index_res10, restaurant_id),
            )

        # Create B-tree indexes on H3 columns for efficient querying
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_restaurants_h3_res8 
        ON restaurants USING btree (h3_index_res8);
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_restaurants_h3_res9 
        ON restaurants USING btree (h3_index_res9);
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_restaurants_h3_res10 
        ON restaurants USING btree (h3_index_res10);
        """)

        conn.commit()
        print(f"H3 indexes created and updated for {len(restaurants)} restaurants")
    except Exception as e:
        conn.rollback()
        print(f"Error initializing H3 indexes: {e}")
    finally:
        cursor.close()
        conn.close()


def find_nearby_restaurants_h3(lat, lng, radius_km):
    """
    Find restaurants near a location using H3 indexing.

    Args:
        lat (float): Latitude of center point
        lng (float): Longitude of center point
        radius_km (float): Search radius in kilometers

    Returns:
        list: Restaurants within the radius, ordered by distance
    """
    # Determine appropriate H3 resolution based on radius
    resolution = get_h3_resolution_for_radius(radius_km)

    # Get the H3 index for the center point
    center_h3 = h3.geo_to_h3(lat, lng, resolution)

    # Get all H3 indexes within the radius (using k-ring)
    # Calculate number of rings needed to cover the radius
    hex_radius = max(1, int(radius_km / (h3.edge_length(resolution, unit="km"))))
    h3_indexes = h3.k_ring(center_h3, hex_radius)

    # Choose appropriate column based on resolution
    if resolution <= 8:
        h3_column = "h3_index_res8"
    elif resolution == 9:
        h3_column = "h3_index_res9"
    else:
        h3_column = "h3_index_res10"

    # Convert h3_indexes to list for query
    h3_index_list = list(h3_indexes)

    if not h3_index_list:
        return []

    # Create placeholders for SQL IN clause
    placeholders = ", ".join(["%s"] * len(h3_index_list))

    # Query restaurants in these cells and calculate exact distance
    query = f"""
    SELECT *, 
        (6371 * acos(cos(radians(%s)) * cos(radians("Latitude")) * cos(radians("Longitude") - 
        radians(%s)) + sin(radians(%s)) * sin(radians("Latitude")))) AS distance 
    FROM restaurants 
    WHERE {h3_column} IN ({placeholders})
    AND (6371 * acos(cos(radians(%s)) * cos(radians("Latitude")) * cos(radians("Longitude") - 
        radians(%s)) + sin(radians(%s)) * sin(radians("Latitude")))) < %s 
    ORDER BY distance;
    """

    # Parameters: [lat, lng, lat, h3_index_list, lat, lng, lat, radius_km]
    params = [lat, lng, lat] + h3_index_list + [lat, lng, lat, radius_km]

    return execute_query(query, params)
