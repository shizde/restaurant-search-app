from flask import Blueprint, jsonify, request
from app.utils.db_utils import execute_query, get_db_connection
import os

# Determine which indexing method to use
INDEXING_METHOD = os.environ.get("INDEXING_METHOD", "basic")

# Create blueprint
bp = Blueprint("search", __name__, url_prefix="/api/search")


@bp.route("/restaurants", methods=["GET"])
def search_restaurants():
    """Search for restaurants by name, cuisine, or location."""
    # Get query parameters
    q = request.args.get("q")
    lat = request.args.get("lat")
    lng = request.args.get("lng")
    radius = request.args.get("radius", default=5.0, type=float)

    # Text search
    if q:
        query = """
        SELECT * FROM restaurants
        WHERE name ILIKE %s 
        OR cuisine ILIKE %s
        OR city ILIKE %s
        LIMIT 50
        """

        search_term = f"%{q}%"
        results = execute_query(query, (search_term, search_term, search_term))

        return jsonify(results)

    # Location-based search
    elif lat and lng:
        try:
            lat, lng = float(lat), float(lng)
        except ValueError:
            return jsonify({"error": "Invalid coordinates"}), 400

        # Use appropriate indexing method based on environment variable
        if INDEXING_METHOD == "h3":
            from app.utils.h3_utils import find_nearby_restaurants_h3

            results = find_nearby_restaurants_h3(lat, lng, radius)
        elif INDEXING_METHOD == "btree":
            from app.utils.btree_utils import find_nearby_restaurants_btree

            results = find_nearby_restaurants_btree(lat, lng, radius)
        elif INDEXING_METHOD == "postgis":
            from app.utils.postgis_utils import find_nearby_restaurants_postgis

            results = find_nearby_restaurants_postgis(lat, lng, radius)
        else:
            # Basic approach with Haversine formula
            query = """
            SELECT *, 
                (6371 * acos(cos(radians(%s)) * cos(radians(latitude)) * cos(radians(longitude) - 
                radians(%s)) + sin(radians(%s)) * sin(radians(latitude)))) AS distance 
            FROM restaurants 
            WHERE (6371 * acos(cos(radians(%s)) * cos(radians(latitude)) * cos(radians(longitude) - 
                radians(%s)) + sin(radians(%s)) * sin(radians(latitude)))) < %s 
            ORDER BY distance;
            """

            results = execute_query(query, (lat, lng, lat, lat, lng, lat, radius))

        return jsonify(results)

    else:
        return jsonify(
            {"error": "Either search query or coordinates are required"}
        ), 400


@bp.route("/nearby", methods=["GET"])
def nearby_restaurants():
    """Find restaurants near a location using the configured indexing method."""
    # Get query parameters
    lat = request.args.get("lat")
    lng = request.args.get("lng")
    radius = request.args.get("radius", default=5.0, type=float)

    if not lat or not lng:
        return jsonify({"error": "Latitude and longitude are required"}), 400

    try:
        lat, lng = float(lat), float(lng)
    except ValueError:
        return jsonify({"error": "Invalid coordinates"}), 400

    # Use appropriate indexing method based on environment variable
    if INDEXING_METHOD == "h3":
        from app.utils.h3_utils import find_nearby_restaurants_h3

        results = find_nearby_restaurants_h3(lat, lng, radius)
    elif INDEXING_METHOD == "btree":
        from app.utils.btree_utils import find_nearby_restaurants_btree

        results = find_nearby_restaurants_btree(lat, lng, radius)
    elif INDEXING_METHOD == "postgis":
        from app.utils.postgis_utils import find_nearby_restaurants_postgis

        results = find_nearby_restaurants_postgis(lat, lng, radius)
    else:
        # Basic approach with Haversine formula
        query = """
        SELECT *, 
            (6371 * acos(cos(radians(%s)) * cos(radians(latitude)) * cos(radians(longitude) - 
            radians(%s)) + sin(radians(%s)) * sin(radians(latitude)))) AS distance 
        FROM restaurants 
        WHERE (6371 * acos(cos(radians(%s)) * cos(radians(latitude)) * cos(radians(longitude) - 
            radians(%s)) + sin(radians(%s)) * sin(radians(latitude)))) < %s 
        ORDER BY distance;
        """

        results = execute_query(query, (lat, lng, lat, lat, lng, lat, radius))

    return jsonify(
        {
            "indexing_method": INDEXING_METHOD,
            "count": len(results),
            "restaurants": results,
        }
    )
