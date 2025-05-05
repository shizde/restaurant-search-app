from flask import Flask, jsonify, request
import os
import importlib

# Initialize Flask app
app = Flask(__name__)

# Get the indexing method from environment variable, default to 'basic'
INDEXING_METHOD = os.environ.get("INDEXING_METHOD", "basic")
valid_methods = ["basic", "btree", "postgis", "h3"]

if INDEXING_METHOD not in valid_methods:
    print(
        f"Warning: Invalid indexing method '{INDEXING_METHOD}'. Using 'basic' instead."
    )
    INDEXING_METHOD = "basic"

print(f"Starting application with indexing method: {INDEXING_METHOD}")

# Import the routes
from app.routes.restaurants import bp as restaurants_bp
from app.routes.users import bp as users_bp
from app.routes.search import bp as search_bp
from app.routes.benchmark import bp as benchmark_bp

# Register blueprints
app.register_blueprint(restaurants_bp)
app.register_blueprint(users_bp)
app.register_blueprint(search_bp)
app.register_blueprint(benchmark_bp)


@app.route("/health", methods=["GET"])
def health_check():
    """Simple health check endpoint."""
    return jsonify({"status": "healthy", "indexing_method": INDEXING_METHOD})


if __name__ == "__main__":
    # Run the Flask app
    app.run(host="0.0.0.0", port=5000, debug=True)
