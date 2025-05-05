from flask import Blueprint, jsonify, request
from app.utils.db_utils import execute_query
import os

# Determine which indexing method to use
INDEXING_METHOD = os.environ.get("INDEXING_METHOD", "basic")

# Create blueprint
bp = Blueprint("restaurants", __name__, url_prefix="/api/restaurants")


@bp.route("", methods=["GET"])
def get_restaurants():
    """Get all restaurants with optional filtering."""
    # Get query parameters
    cuisine = request.args.get("cuisine")
    price = request.args.get("price")
    city = request.args.get("city")

    # Build query
    query = "SELECT * FROM restaurants WHERE 1=1"
    params = []

    # Add filters
    if cuisine:
        query += " AND cuisine ILIKE %s"
        params.append(f"%{cuisine}%")

    if price:
        query += " AND price = %s"
        params.append(price)

    if city:
        query += " AND city ILIKE %s"
        params.append(f"%{city}%")

    # Add pagination
    limit = request.args.get("limit", default=20, type=int)
    offset = request.args.get("offset", default=0, type=int)

    query += " ORDER BY name LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    # Execute query
    restaurants = execute_query(query, params)

    return jsonify(restaurants)


@bp.route("/<int:restaurant_id>", methods=["GET"])
def get_restaurant(restaurant_id):
    """Get a specific restaurant by ID."""
    query = "SELECT * FROM restaurants WHERE restaurant_id = %s"
    restaurant = execute_query(query, (restaurant_id,), fetch_all=False)

    if restaurant:
        # Get ratings for this restaurant
        ratings_query = """
        SELECT AVG(rating) as avg_rating, COUNT(*) as rating_count
        FROM ratings
        WHERE place_id = %s
        """

        ratings = execute_query(ratings_query, (restaurant_id,), fetch_all=False)

        # Add ratings to restaurant data
        restaurant_data = dict(restaurant)
        if ratings:
            restaurant_data["avg_rating"] = (
                float(ratings["avg_rating"]) if ratings["avg_rating"] else None
            )
            restaurant_data["rating_count"] = ratings["rating_count"]

        return jsonify(restaurant_data)
    else:
        return jsonify({"error": "Restaurant not found"}), 404


@bp.route("/nearby", methods=["GET"])
def get_nearby_restaurants():
    """Find restaurants near a specific location."""
    # Get query parameters
    lat = request.args.get("lat")
    lng = request.args.get("lng")
    radius = request.args.get("radius", default=5.0, type=float)

    if not lat or not lng:
        return jsonify({"error": "Latitude and longitude are required"}), 400

    # Convert to float
    try:
        lat = float(lat)
        lng = float(lng)
    except ValueError:
        return jsonify({"error": "Invalid coordinates"}), 400

    # Use the appropriate method based on the indexing strategy
    if INDEXING_METHOD == "h3":
        from app.utils.h3_utils import find_nearby_restaurants_h3

        restaurants = find_nearby_restaurants_h3(lat, lng, radius)
    elif INDEXING_METHOD == "btree":
        from app.utils.btree_utils import find_nearby_restaurants_btree

        restaurants = find_nearby_restaurants_btree(lat, lng, radius)
    elif INDEXING_METHOD == "postgis":
        from app.utils.postgis_utils import find_nearby_restaurants_postgis

        restaurants = find_nearby_restaurants_postgis(lat, lng, radius)
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

        restaurants = execute_query(query, (lat, lng, lat, lat, lng, lat, radius))

    # Add method used to the response
    return jsonify(
        {
            "indexing_method": INDEXING_METHOD,
            "center": {"lat": lat, "lng": lng},
            "radius_km": radius,
            "count": len(restaurants),
            "restaurants": restaurants,
        }
    )
