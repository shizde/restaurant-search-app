#!/usr/bin/env python3
import os
from app.utils.db_utils import get_db_connection
from scripts.init_basic import init_basic_db
from app.utils.h3_utils import initialize_h3_indexes


def init_h3_db():
    """Initialize database with H3 indexes."""
    # First initialize with basic setup
    init_basic_db()

    # Then create H3 indexes
    initialize_h3_indexes()

    print("H3 database initialization completed")


if __name__ == "__main__":
    init_h3_db()
