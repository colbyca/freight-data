from datetime import datetime
import os
import time
import psycopg2
import concurrent.futures
import subprocess
import argparse
import tempfile
import math

def split_file_into_chunks(file_path, num_chunks):
    """Split a large file into roughly equal chunks and return temp file paths"""
    total_lines = 0
    with open(file_path, 'r') as f:
        for _ in f:
            total_lines += 1
    
    lines_per_chunk = math.ceil(total_lines / num_chunks)
    
    temp_files = []
    with open(file_path, 'r') as source:
        for i in range(num_chunks):
            temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv')
            temp_files.append(temp_file.name)
            
            # Write chunk of lines to temp file
            for _ in range(lines_per_chunk):
                line = source.readline()
                if not line:  # End of file
                    break
                temp_file.write(line)
            
            temp_file.close()
    
    return temp_files

def setup_database(conn_params, month):
    """Setup database schema and extensions"""
    conn_string = f"host={conn_params['host']} port={conn_params['port']} dbname={conn_params['dbname']} user={conn_params['user']} password={conn_params['password']}"
    print(conn_string)
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Enable PostGIS
    cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    
    # Create table if not exists
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS month_{month:02d}_routes (
        id SERIAL PRIMARY KEY,
        truck_id VARCHAR(50) NOT NULL,
        location GEOGRAPHY(POINT) NOT NULL,
        timestamp BIGINT NOT NULL,
        speed NUMERIC,
        is_valid BOOLEAN NOT NULL,
        collection_date DATE NOT NULL
    );
    """)
    
    # Create indexes (for after data is loaded)
    cursor.close()
    conn.close()
    
    print("Database schema ready.")

def prepare_temp_files_for_copy(input_file, output_file, worker_id):
    """Process input file to create a COPY-compatible format with transformed data"""
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for i, line in enumerate(infile):
            try:
                parts = line.strip().split(';')
                if len(parts) >= 6:
                    truck_id = parts[0]
                    latitude = parts[1]
                    longitude = parts[2]
                    timestamp = parts[3]
                    speed = parts[4]
                    is_valid = parts[5] == '1'
                    
                    # Create a tab-separated line ready for COPY
                    # Format: truck_id, WKT point, timestamp, speed, is_valid, collection_date
                    wkt_point = f"SRID=4326;POINT({longitude} {latitude})"
                    collection_date = datetime.fromtimestamp(int(timestamp)).date()
                    
                    outfile.write(f"{truck_id},{wkt_point},{timestamp},{speed},{is_valid},{collection_date}\n")
                    if i % 100000 == 0 and worker_id == 1:
                        print(f"Processed {i} lines...")

            except Exception as e:
                print(f"Error processing line: {line.strip()}, Error: {str(e)}")
                continue

def load_chunk(chunk_file, conn_params, worker_id, month):
    """Load a single chunk of data using PostgreSQL's COPY command"""
    try:
        # Process chunk file into a COPY-compatible format
        processed_file = f"{chunk_file}.processed"
        prepare_temp_files_for_copy(chunk_file, processed_file, worker_id)
        
        # Use psql command for fastest loading
        copy_command = f"""\\COPY month_{month:02d}_routes (truck_id, location, timestamp, speed, is_valid, collection_date) 
                           FROM '{processed_file}' WITH (FORMAT csv, DELIMITER E',', QUOTE '"', ESCAPE '\\', NULL '\\N')"""
        
        # Execute the COPY command using psql
        cmd = [
            r"psql",  # Assuming psql is available in the Docker container's PATH
            "-h", conn_params['host'],
            "-p", str(conn_params['port']),
            "-d", conn_params['dbname'],
            "-U", conn_params['user'],
            "-c", copy_command
        ]
        
        # Set PGPASSWORD environment variable for passwordless connection
        env = os.environ.copy()
        env["PGPASSWORD"] = conn_params['password']
        
        # Execute command
        start_time = time.time()
        print(f"running COPY command for Worker {worker_id}")
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        print(f"DONE running COPY command for Worker {worker_id}")
        
        if result.returncode != 0:
            print(f"Worker {worker_id} error: {result.stderr}")
            return 0
        
        # Extract number of rows copied
        output = result.stdout
        rows_copied = 0
        for line in output.split('\n'):
            if "COPY" in line:
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        rows_copied = int(parts[1])
                    except ValueError:
                        pass
        
        elapsed = time.time() - start_time
        rate = rows_copied / elapsed if elapsed > 0 else 0
        print(f"Worker {worker_id}: Loaded {rows_copied} rows in {elapsed:.2f}s ({rate:.2f} rows/sec)")
        
        # Clean up temp files
        os.unlink(processed_file)
        os.unlink(chunk_file)
        
        return rows_copied
    
    except Exception as e:
        print(f"Worker {worker_id} exception: {str(e)}")
        return 0

def create_indexes(conn_params, month):
    """Create indexes after data is loaded for better performance"""
    conn_string = f"host={conn_params['host']} port={conn_params['port']} dbname={conn_params['dbname']} user={conn_params['user']} password={conn_params['password']}"
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cursor = conn.cursor()
    
    print("Creating indexes (this may take a few minutes)...")
    
    # Create indexes if they don't exist
    cursor.execute(f"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_truck_id') THEN
            CREATE INDEX idx_truck_id ON month_{month:02d}_routes(truck_id);
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_timestamp') THEN
            CREATE INDEX idx_timestamp ON month_{month:02d}_routes(timestamp);
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_collection_date') THEN
            CREATE INDEX idx_collection_date ON month_{month:02d}_routes(collection_date);
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_location') THEN
            CREATE INDEX idx_location ON month_{month:02d}_routes USING GIST(location);
        END IF;
    END $$;
    """)
    
    cursor.close()
    conn.close()
    print("Indexes created successfully.")

def main():
    parser = argparse.ArgumentParser(description='Parallel data loader for freight routes into PostGIS')
    parser.add_argument('--month', type=int, required=True, help='Month number (1-12)')
    parser.add_argument('--host', type=str, default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--dbname', type=str, default='mydatabase', help='Database name')
    parser.add_argument('--user', type=str, default='postgres', help='Database user')
    parser.add_argument('--password', type=str, required=True, help='Database password')
    parser.add_argument('--workers', type=int, default=0, 
                        help='Number of parallel workers (0=auto based on CPU count)')
    
    args = parser.parse_args()
    
    # Determine number of workers
    workers = args.workers if args.workers > 0 else os.cpu_count()
    
    # Connection parameters
    conn_params = {
        'host': args.host,
        'port': args.port,
        'dbname': args.dbname,
        'user': args.user,
        'password': args.password
    }
    
    # Construct file path
    file_path = os.path.join("monthly_route_data", "routes", f"month_{args.month:02d}.csv")
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} does not exist")
        return
    
    # Setup database schema
    setup_database(conn_params, args.month)
    
    # Split the file
    print(f"Splitting file into {workers} chunks...")
    chunk_files = split_file_into_chunks(file_path, workers)
    
    # Load data in parallel
    print(f"Starting parallel load with {workers} workers...")
    start_time = time.time()
    total_rows = 0
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        # Submit all loading tasks
        futures = []
        for i, chunk_file in enumerate(chunk_files):
            future = executor.submit(load_chunk, chunk_file, conn_params, i+1, args.month)
            futures.append(future)
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(futures):
            rows = future.result()
            total_rows += rows
    
    total_time = time.time() - start_time
    avg_rate = total_rows / total_time if total_time > 0 else 0
    
    print(f"\nLoading complete!")
    print(f"Total rows: {total_rows:,}")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Average rate: {avg_rate:.2f} rows/second")
    
    # Create indexes after data is loaded
    create_indexes(conn_params, args.month)

if __name__ == "__main__":
    main()