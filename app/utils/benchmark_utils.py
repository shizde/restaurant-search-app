import time
import statistics
from app.utils.db_utils import get_db_connection


def benchmark_nearby_search(lat, lng, radius_km, method, num_runs=5):
    """
    Benchmark the performance of a nearby search method.

    Args:
        lat (float): Latitude of center point
        lng (float): Longitude of center point
        radius_km (float): Search radius in kilometers
        method (str): Indexing method to benchmark ('basic', 'btree', 'postgis', 'h3')
        num_runs (int): Number of runs to average over

    Returns:
        dict: Benchmark results including timing and result counts
    """
    # Import the appropriate search function
    if method == "h3":
        from app.utils.h3_utils import find_nearby_restaurants_h3 as search_func
    elif method == "btree":
        from app.utils.btree_utils import find_nearby_restaurants_btree as search_func
    elif method == "postgis":
        from app.utils.postgis_utils import (
            find_nearby_restaurants_postgis as search_func,
        )
    else:
        # Basic search function
        def search_func(lat, lng, radius_km):
            conn = get_db_connection()
            cursor = conn.cursor()
            try:
                query = """
                SELECT *, 
                    (6371 * acos(cos(radians(%s)) * cos(radians(latitude)) * cos(radians(longitude) - 
                    radians(%s)) + sin(radians(%s)) * sin(radians(latitude)))) AS distance 
                FROM restaurants 
                WHERE (6371 * acos(cos(radians(%s)) * cos(radians(latitude)) * cos(radians(longitude) - 
                    radians(%s)) + sin(radians(%s)) * sin(radians(latitude)))) < %s 
                ORDER BY distance;
                """

                cursor.execute(query, (lat, lng, lat, lat, lng, lat, radius_km))
                results = cursor.fetchall()

                # Convert to list of dicts
                restaurants = []
                for row in results:
                    restaurant = {}
                    for i, col in enumerate(cursor.description):
                        restaurant[col[0]] = row[i]
                    restaurants.append(restaurant)

                return restaurants
            finally:
                cursor.close()
                conn.close()

    # Run the benchmark
    run_times = []
    result_counts = []

    for i in range(num_runs):
        start_time = time.time()
        results = search_func(lat, lng, radius_km)
        end_time = time.time()

        run_time = end_time - start_time
        run_times.append(run_time)
        result_counts.append(len(results))

    # Calculate statistics
    avg_time = statistics.mean(run_times)
    min_time = min(run_times)
    max_time = max(run_times)

    return {
        "method": method,
        "num_runs": num_runs,
        "avg_time_seconds": avg_time,
        "min_time_seconds": min_time,
        "max_time_seconds": max_time,
        "avg_result_count": statistics.mean(result_counts),
        "run_times": run_times,
    }
