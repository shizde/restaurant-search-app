import os
import time
import math
import psycopg2
from psycopg2.extras import RealDictCursor

# Database connection parameters
DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "restaurants")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "postgres")


def get_db_connection():
    """Create a database connection with retry logic."""
    max_retries = 10
    retry_delay = 1  # seconds

    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
            )
            print(
                f"Database connection established successfully on attempt {attempt + 1}"
            )
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                print(
                    f"Database connection failed (attempt {attempt + 1} of {max_retries}): {e}"
                )
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 1.5  # Increase delay for each retry
            else:
                print(
                    f"Maximum retries ({max_retries}) exceeded. Could not connect to database."
                )
                raise e


def get_db_connection_with_retry():
    """Alias for get_db_connection for backward compatibility."""
    return get_db_connection()


def execute_query(query, params=None, fetch_all=True, dict_cursor=True):
    """Execute a database query and return results."""
    conn = get_db_connection()
    if dict_cursor:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
    else:
        cursor = conn.cursor()

    try:
        cursor.execute(query, params or ())

        if fetch_all:
            result = cursor.fetchall()
        else:
            result = cursor.fetchone()

        conn.commit()
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of earth in kilometers

    return c * r
