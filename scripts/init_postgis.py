#!/usr/bin/env python3
import os
from app.utils.db_utils import get_db_connection
from scripts.init_basic import init_basic_db
from app.utils.postgis_utils import initialize_postgis_indexes


def init_postgis_db():
    """Initialize database with PostGIS indexes."""
    # First initialize with basic setup
    init_basic_db()

    # Then create PostGIS indexes
    initialize_postgis_indexes()

    print("PostGIS database initialization completed")


if __name__ == "__main__":
    init_postgis_db()
