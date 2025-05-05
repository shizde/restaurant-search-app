from flask import Blueprint, jsonify, request
from app.utils.db_utils import execute_query

# Create blueprint
bp = Blueprint("users", __name__, url_prefix="/api/users")


@bp.route("", methods=["GET"])
def get_users():
    """Get all users with optional filtering."""
    # Get query parameters
    drink_level = request.args.get("drink_level")
    marital_status = request.args.get("marital_status")

    # Build query
    query = "SELECT * FROM users WHERE 1=1"
    params = []

    # Add filters
    if drink_level:
        query += " AND drink_level = %s"
        params.append(drink_level)

    if marital_status:
        query += " AND marital_status = %s"
        params.append(marital_status)

    # Add pagination
    limit = request.args.get("limit", default=20, type=int)
    offset = request.args.get("offset", default=0, type=int)

    query += " ORDER BY user_id LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    # Execute query
    users = execute_query(query, params)

    return jsonify(users)


@bp.route("/<user_id>", methods=["GET"])
def get_user(user_id):
    """Get a specific user by ID."""
    query = "SELECT * FROM users WHERE user_id = %s"
    user = execute_query(query, (user_id,), fetch_all=False)

    if user:
        # Get ratings from this user
        ratings_query = """
        SELECT r.*, rest.name as restaurant_name
        FROM ratings r
        JOIN restaurants rest ON r.place_id = rest.restaurant_id
        WHERE r.user_id = %s
        ORDER BY r.rating DESC
        """

        ratings = execute_query(ratings_query, (user_id,))

        # Add ratings to user data
        user_data = dict(user)
        user_data["ratings"] = ratings

        return jsonify(user_data)
    else:
        return jsonify({"error": "User not found"}), 404
