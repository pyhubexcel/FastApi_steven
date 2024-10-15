import psycopg2
import pandas as pd

# Define connection string
DB_CONNECTION_STRING = "postgresql://user:password@localhost:5401/mlsgrid"

# SQL query template for extracting 10 sample records per OriginatingSystemName
query_template = """
    SELECT * FROM {table_name}
    WHERE "OriginatingSystemName" = %s
    LIMIT 10;
"""

# Define table names to extract data from
tables = ["merged_member", "merged_office", "merged_openhouse", "merged_property"]

# Function to extract data from a specific table based on the OriginatingSystemName
def extract_samples(connection, table_name, originating_system_name):
    query = query_template.format(table_name=table_name)
    df = pd.read_sql_query(query, connection, params=(originating_system_name,))
    return df

# Function to merge results for each OriginatingSystemName
def merge_results(connection, originating_system_name):
    merged_data = {}
    for table in tables:
        sample_data = extract_samples(connection, table, originating_system_name)
        merged_data[table] = sample_data
    return merged_data

# Main function to process the data
def process_and_export_samples():
    # Connect to the PostgreSQL database
    with psycopg2.connect(DB_CONNECTION_STRING) as conn:
        # Find all distinct OriginatingSystemName values
        distinct_origins_query = "SELECT DISTINCT \"OriginatingSystemName\" FROM merged_property"
        origins = pd.read_sql_query(distinct_origins_query, conn)['OriginatingSystemName'].tolist()

        for origin in origins:
            # Extract and merge data for each OriginatingSystemName
            merged_results = merge_results(conn, origin)

            # Export the merged results for each table to CSV
            for table_name, data in merged_results.items():
                output_file = f"{table_name}_{origin}_sample.csv"
                data.to_csv(output_file, index=False)
                print(f"Exported {output_file}")

# Execute the function to process and export samples
process_and_export_samples()

