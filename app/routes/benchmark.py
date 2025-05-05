from flask import Blueprint, jsonify, request
from app.utils.benchmark_utils import benchmark_nearby_search

# Create blueprint
bp = Blueprint("benchmark", __name__, url_prefix="/api/benchmark")


@bp.route("/nearby", methods=["GET"])
def benchmark_nearby():
    """Benchmark nearby restaurant search with different methods."""
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

    # Get methods to benchmark
    methods = request.args.get("methods", "basic,btree,postgis,h3").split(",")
    num_runs = int(request.args.get("runs", 3))

    # Run benchmarks
    results = {}
    for method in methods:
        if method in ["basic", "btree", "postgis", "h3"]:
            results[method] = benchmark_nearby_search(
                lat, lng, radius, method, num_runs
            )

    return jsonify(
        {
            "center": {"lat": lat, "lng": lng},
            "radius_km": radius,
            "num_runs": num_runs,
            "results": results,
        }
    )
