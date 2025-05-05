#!/usr/bin/env python3
import os
import csv
from app.utils.db_utils import get_db_connection


def get_csv_column_names(file_path):
    """Get column names from a CSV file."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)  # Get the first row (headers)
            return headers
    except Exception as e:
        print(f"Error reading CSV headers from {file_path}: {e}")
        return []


def init_basic_db():
    """Initialize the database with basic setup based on CSV column names."""
    print("Initializing database with basic setup...")

    # First, determine the column names from CSV files
    restaurants_csv_path = "/app/data/Restaurants.csv"
    users_csv_path = "/app/data/Users.csv"
    ratings_csv_path = "/app/data/Ratings.csv"

    # Get column names from CSV files
    restaurant_columns = get_csv_column_names(restaurants_csv_path)
    user_columns = get_csv_column_names(users_csv_path)
    rating_columns = get_csv_column_names(ratings_csv_path)

    if not restaurant_columns or not user_columns or not rating_columns:
        print("Error: Could not read CSV headers. Aborting database initialization.")
        return

    # Print the detected column names for debugging
    print(f"Restaurant columns detected: {restaurant_columns}")
    print(f"User columns detected: {user_columns}")
    print(f"Rating columns detected: {rating_columns}")

    # Create database connection
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Drop existing tables if they exist (to clean slate)
        cursor.execute("DROP TABLE IF EXISTS ratings;")
        cursor.execute("DROP TABLE IF EXISTS users;")
        cursor.execute("DROP TABLE IF EXISTS restaurants;")

        # Determine the primary key column for restaurants
        restaurant_pk = restaurant_columns[
            0
        ]  # Assuming first column is the primary key

        # Create restaurants table with properly quoted column names
        restaurant_columns_sql = []
        for col in restaurant_columns:
            # Quote the column name to preserve case
            quoted_col = f'"{col}"'
            if col == restaurant_pk:
                restaurant_columns_sql.append(f"{quoted_col} INTEGER PRIMARY KEY")
            elif col in ["Latitude", "Longitude"]:
                # Increased precision and scale to handle larger values
                restaurant_columns_sql.append(f"{quoted_col} DECIMAL(15, 10)")
            elif col == "Franchise":
                restaurant_columns_sql.append(f"{quoted_col} BOOLEAN")
            else:
                restaurant_columns_sql.append(
                    f"{quoted_col} TEXT"
                )  # Using TEXT for all string columns

        restaurants_sql = f"""
        CREATE TABLE restaurants (
            {", ".join(restaurant_columns_sql)}
        );
        """
        cursor.execute(restaurants_sql)

        # Determine the primary key column for users
        user_pk = user_columns[0]  # Assuming first column is the primary key

        # Create users table with properly quoted column names
        user_columns_sql = []
        for col in user_columns:
            # Quote the column name to preserve case
            quoted_col = f'"{col}"'
            if col == user_pk:
                user_columns_sql.append(f"{quoted_col} VARCHAR(50) PRIMARY KEY")
            elif col in ["Latitude", "Longitude"]:
                # Increased precision and scale
                user_columns_sql.append(f"{quoted_col} DECIMAL(15, 10)")
            elif col == "Smoker":
                user_columns_sql.append(f"{quoted_col} BOOLEAN")
            elif col in ["Weight", "BirthYear"]:
                user_columns_sql.append(f"{quoted_col} INTEGER")
            elif col == "Height":
                user_columns_sql.append(f"{quoted_col} FLOAT")
            elif col in ["CuisinePreferences", "PaymentMethods"]:
                # Use TEXT for potentially long string fields
                user_columns_sql.append(f"{quoted_col} TEXT")
            else:
                user_columns_sql.append(
                    f"{quoted_col} TEXT"
                )  # Using TEXT for all other string columns

        users_sql = f"""
        CREATE TABLE users (
            {", ".join(user_columns_sql)}
        );
        """
        cursor.execute(users_sql)

        # Create ratings table
        # Find foreign key columns
        restaurant_ref_col = None
        user_ref_col = None
        for col in rating_columns:
            if "place" in col.lower() or "restaurant" in col.lower():
                restaurant_ref_col = col
            elif "user" in col.lower():
                user_ref_col = col

        if not restaurant_ref_col or not user_ref_col:
            print("Error: Could not identify foreign key columns in ratings table")
            return

        rating_columns_sql = ['"id" SERIAL PRIMARY KEY']
        for col in rating_columns:
            # Quote the column name to preserve case
            quoted_col = f'"{col}"'
            if col == restaurant_ref_col:
                rating_columns_sql.append(f"{quoted_col} INTEGER")
            elif col == user_ref_col:
                rating_columns_sql.append(f"{quoted_col} VARCHAR(50)")
            elif "rating" in col.lower():
                rating_columns_sql.append(f"{quoted_col} INTEGER")
            else:
                rating_columns_sql.append(
                    f"{quoted_col} TEXT"
                )  # Text for any other columns

        # Add foreign key constraints with quoted column names
        rating_columns_sql.append(
            f'FOREIGN KEY ("{restaurant_ref_col}") REFERENCES restaurants ("{restaurant_pk}")'
        )
        rating_columns_sql.append(
            f'FOREIGN KEY ("{user_ref_col}") REFERENCES users ("{user_pk}")'
        )

        ratings_sql = f"""
        CREATE TABLE ratings (
            {", ".join(rating_columns_sql)}
        );
        """
        cursor.execute(ratings_sql)

        # Import data from CSVs
        import_data(
            conn,
            cursor,
            restaurant_columns,
            user_columns,
            rating_columns,
            restaurants_csv_path,
            users_csv_path,
            ratings_csv_path,
        )

        conn.commit()
        print("Database schema created successfully based on CSV column names")
    except Exception as e:
        conn.rollback()
        print(f"Error initializing database: {e}")
    finally:
        cursor.close()
        conn.close()


def import_data(
    conn,
    cursor,
    restaurant_columns,
    user_columns,
    rating_columns,
    restaurants_csv_path,
    users_csv_path,
    ratings_csv_path,
):
    """Import data from CSV files using detected column structure."""
    try:
        # Import restaurants
        with open(restaurants_csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Prepare placeholders and values
                placeholders = ", ".join(["%s"] * len(restaurant_columns))
                # Quote column names
                columns = ", ".join([f'"{col}"' for col in restaurant_columns])
                values = []

                for col in restaurant_columns:
                    if col == "Franchise":
                        # Convert to boolean
                        franchise = row.get(col, "").lower() in (
                            "true",
                            "t",
                            "yes",
                            "y",
                            "1",
                        )
                        values.append(franchise)
                    elif col in ["Latitude", "Longitude"]:
                        # Convert to float
                        try:
                            values.append(float(row.get(col, 0) or 0))
                        except (ValueError, TypeError):
                            values.append(0.0)  # Default to 0 if conversion fails
                    elif col == restaurant_columns[0]:  # Primary key column
                        # Ensure it's an integer
                        try:
                            values.append(int(row.get(col, 0) or 0))
                        except (ValueError, TypeError):
                            values.append(0)  # Default to 0 if conversion fails
                    else:
                        values.append(row.get(col, ""))

                # Insert the data
                cursor.execute(
                    f"INSERT INTO restaurants ({columns}) VALUES ({placeholders})",
                    values,
                )

        conn.commit()
        print("Restaurants data imported successfully")

        # Import users
        with open(users_csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Prepare placeholders and values
                placeholders = ", ".join(["%s"] * len(user_columns))
                # Quote column names
                columns = ", ".join([f'"{col}"' for col in user_columns])
                values = []

                for col in user_columns:
                    if col == "Smoker":
                        # Convert to boolean
                        smoker = row.get(col, "").lower() in (
                            "true",
                            "t",
                            "yes",
                            "y",
                            "1",
                        )
                        values.append(smoker)
                    elif col in ["Latitude", "Longitude"]:
                        # Convert to float
                        try:
                            values.append(float(row.get(col, 0) or 0))
                        except (ValueError, TypeError):
                            values.append(0.0)  # Default to 0 if conversion fails
                    elif col in ["Weight", "BirthYear"]:
                        # Convert to integer
                        try:
                            values.append(int(row.get(col, 0) or 0))
                        except (ValueError, TypeError):
                            values.append(0)  # Default to 0 if conversion fails
                    elif col == "Height":
                        # Convert to float
                        try:
                            values.append(float(row.get(col, 0) or 0))
                        except (ValueError, TypeError):
                            values.append(0.0)  # Default to 0 if conversion fails
                    else:
                        values.append(row.get(col, ""))

                # Insert the data
                cursor.execute(
                    f"INSERT INTO users ({columns}) VALUES ({placeholders})", values
                )

        conn.commit()
        print("Users data imported successfully")

        # Import ratings
        with open(ratings_csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    # Prepare placeholders and values
                    placeholders = ", ".join(["%s"] * len(rating_columns))
                    # Quote column names
                    columns = ", ".join([f'"{col}"' for col in rating_columns])
                    values = []

                    for col in rating_columns:
                        if "rating" in col.lower() or "place" in col.lower():
                            # Convert to integer
                            try:
                                values.append(int(row.get(col, 0) or 0))
                            except (ValueError, TypeError):
                                values.append(0)  # Default to 0 if conversion fails
                        else:
                            values.append(row.get(col, ""))

                    # Insert the data
                    cursor.execute(
                        f"INSERT INTO ratings ({columns}) VALUES ({placeholders})",
                        values,
                    )
                except Exception as e:
                    # Skip invalid ratings
                    print(f"Skipping rating due to error: {e}")
                    continue

        conn.commit()
        print("Ratings data imported successfully")
    except Exception as e:
        conn.rollback()
        print(f"Error importing data: {e}")


if __name__ == "__main__":
    init_basic_db()
