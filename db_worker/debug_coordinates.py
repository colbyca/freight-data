import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

def get_db_connection():
    """Create a database connection using environment variables"""
    conn_params = {
        'host': 'localhost',
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'dbname': os.getenv('POSTGRES_DB', 'mydatabase'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'password')
    }
    
    conn_string = f"host={conn_params['host']} port={conn_params['port']} dbname={conn_params['dbname']} user={conn_params['user']} password={conn_params['password']}"
    return psycopg2.connect(conn_string)

def save_coordinates_to_file(query, output_file):
    """Execute a SQL query and save coordinates to a file"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        print(f"Executing query...")
        cursor.execute(query)
        
        # Fetch results
        results = cursor.fetchall()
        
        # Save to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{output_file}_{timestamp}.csv"
        
        with open(filename, 'w') as f:
            f.write("datetime,longitude,latitude\n")
            for row in results:
                # Assuming first column is longitude and second is latitude
                f.write(f"{row[0]},{row[1]},{row[2]}\n")
        
        print(f"Saved {len(results)} coordinates to {filename}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    # Example query - modify this as needed
#     example_query = """
# WITH ranked_points AS (
#     SELECT 
#         *,
#         ROW_NUMBER() OVER (PARTITION BY truck_id ORDER BY timestamp) AS rn
#     FROM month_01_routes
#     WHERE truck_id = 'id_1350125185'
# )
# SELECT 
#         to_timestamp(timestamp)::timestamp as datetime,
#         ST_X(location::geometry) as longitude,
#         ST_Y(location::geometry) as latitude
# FROM ranked_points
# WHERE rn % 5 = 1;
# """
    example_query = """
    SELECT 
        to_timestamp(timestamp)::timestamp as datetime,
        ST_X(location::geometry) as longitude,
        ST_Y(location::geometry) as latitude
    FROM test_routes
    WHERE route_id = 1
    """ 
    
    save_coordinates_to_file(example_query, "debug_coordinates") 