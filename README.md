# Restaurant Search Application

## Overview

This project is a Flask-based web application for querying restaurant data from a PostgreSQL database. It demonstrates different spatial indexing methods for efficient geographic searches. The application provides endpoints for restaurant data, user information, and location-based searches.

## Features

- REST API for restaurants, users, and ratings data
- Four different spatial indexing approaches for location-based searches:
  - Basic (no spatial indexing)
  - B-tree indexing on latitude/longitude
  - PostGIS/Quad-tree spatial indexing
  - H3 hexagonal hierarchical indexing
- Docker and Docker Compose setup for each indexing method
- Support for importing data from CSV files

## Project Structure

```
restaurant-search-app/
├── Dockerfile.basic            # Basic setup without spatial indexing
├── Dockerfile.btree            # Setup with B-tree indexing
├── Dockerfile.postgis          # Setup with PostGIS/Quad-tree indexing
├── Dockerfile.h3               # Setup with H3 indexing
├── docker-compose.yml          # Default compose file
├── docker-compose.btree.yml    # B-tree compose file
├── docker-compose.postgis.yml  # PostGIS compose file
├── docker-compose.h3.yml       # H3 compose file
├── requirements.txt            # Python dependencies
├── app/
│   ├── init.py
│   ├── main.py                 # Main Flask application
│   ├── utils/
│   │   ├── init.py
│   │   ├── db_utils.py         # Database utilities
│   │   ├── btree_utils.py      # B-tree indexing utilities
│   │   ├── postgis_utils.py    # PostGIS indexing utilities
│   │   ├── h3_utils.py         # H3 indexing utilities
│   │   └── benchmark_utils.py  # Performance benchmarking utilities
│   └── routes/
│       ├── init.py
│       ├── restaurants.py      # Restaurant endpoints
│       ├── users.py            # User endpoints
│       ├── search.py           # Search endpoints
│       └── benchmark.py        # Benchmarking endpoints
├── scripts/
│   ├── init_basic.py           # Basic database initialization
│   ├── init_btree.py           # B-tree initialization
│   ├── init_postgis.py         # PostGIS initialization
│   └── init_h3.py              # H3 initialization
└── data/
├── Restaurants.csv         # Restaurant data
├── Users.csv               # User data
└── Ratings.csv             # Rating data
```

## Spatial Indexing Methods

### 1. Basic (No Indexing)

The basic setup uses a simple Haversine formula in SQL to calculate distances between points. This approach works for small datasets but becomes inefficient for larger datasets as it requires a full table scan.

### 2. B-tree Indexing

B-tree indexing creates standard indexes on latitude and longitude columns. It improves performance by pre-filtering results based on coordinate ranges before applying the Haversine formula for exact distance calculation.

### 3. PostGIS/Quad-tree Indexing

This approach uses PostGIS extension with GiST (Generalized Search Tree) spatial indexes. It's specifically designed for geospatial data and provides efficient operations like distance calculations and containment tests.

### 4. H3 Hexagonal Hierarchical Indexing

H3 indexing divides the earth into hexagonal cells at different resolutions. This method provides efficient proximity searches by converting coordinates to H3 indexes and querying only the relevant cells.

## Prerequisites

- Docker and Docker Compose
- Restaurant data in CSV format (must include latitude and longitude)

## Getting Started

1. Place your CSV data files in the `data/` directory:
   - Restaurants.csv
   - Users.csv
   - Ratings.csv

2. Choose an indexing method and start the application:

   - Basic:
     ```bash
     docker-compose up --build
     ```

   - B-tree:
     ```bash
     docker-compose -f docker-compose.btree.yml up --build
     ```

   - PostGIS/Quad-tree:
     ```bash
     docker-compose -f docker-compose.postgis.yml up --build
     ```

   - H3:
     ```bash
     docker-compose -f docker-compose.h3.yml up --build
     ```

## API Endpoints

### Restaurants

- `GET /api/restaurants`: List all restaurants (with optional filtering)
- `GET /api/restaurants/{id}`: Get a specific restaurant by ID
- `GET /api/restaurants/nearby`: Find restaurants near a location

### Users

- `GET /api/users`: List all users (with optional filtering)
- `GET /api/users/{id}`: Get a specific user by ID

### Search

- `GET /api/search/restaurants`: Search restaurants by name, cuisine, or location
- `GET /api/search/nearby`: Find restaurants near a location (alternative endpoint)

### Benchmarking

- `GET /api/benchmark/nearby`: Benchmark the performance of different indexing methods

## Performance Comparison

Each indexing method has different performance characteristics:

- **Basic**: Suitable for small datasets (<1,000 records)
- **B-tree**: Good for medium-sized datasets (~10,000 records)
- **PostGIS**: Excellent for large datasets and complex spatial queries
- **H3**: Superior for specific use cases like finding points within a radius

## License

This project is provided under the MIT License.