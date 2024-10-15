import psycopg2

# Database connection parameters
DB_CONNECTION_STRING = "postgresql://user:password@localhost:5401/mlsgrid"

# Output file
OUTPUT_FILE = "merged_tables.txt"

# Establish connection to the database
def connect_to_db():
    try:
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

# Fetch table fields, types, and sizes
def fetch_merged_table_info(conn):
    cursor = conn.cursor()

    query = """
    SELECT table_name, column_name, data_type, character_maximum_length
    FROM information_schema.columns
    WHERE table_name LIKE 'merged_%'
    ORDER BY table_name, ordinal_position;
    """

    cursor.execute(query)
    return cursor.fetchall()

# Write table information to file
def write_to_file(data):
    with open(OUTPUT_FILE, 'w') as f:
        current_table = None
        for row in data:
            table_name, column_name, data_type, char_length = row
            if table_name != current_table:
                if current_table:
                    f.write("\n")
                f.write(f"Table: {table_name}\n")
                current_table = table_name
            length_info = f" (Length: {char_length})" if char_length else ""
            f.write(f"    - {column_name}: {data_type}{length_info}\n")

# Main function
def main():
    # Connect to the database
    conn = connect_to_db()
    if not conn:
        return

    try:
        # Fetch table fields
        data = fetch_merged_table_info(conn)
        # Write data to file
        write_to_file(data)
        print(f"Table information has been written to {OUTPUT_FILE}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()

