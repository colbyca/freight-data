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
    CREATE TABLE IF NOT EXISTS month_{month:02d}_stops (
        id SERIAL PRIMARY KEY,
        stop_id VARCHAR(50) NOT NULL,
        address TEXT NOT NULL,
        location GEOGRAPHY(POINT) NOT NULL,
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP NOT NULL,
        duration_minutes INTEGER NOT NULL
    );
    """)
    
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
                    stop_id = parts[0]
                    address = parts[1].replace('"', '\\"')  # Escape any double quotes in address
                    latitude = parts[2]
                    longitude = parts[3]
                    start_time = datetime.strptime(parts[4], '%Y-%m-%d %H:%M:%S')
                    end_time = datetime.strptime(parts[5], '%Y-%m-%d %H:%M:%S')
                    
                    # Calculate duration in minutes
                    duration = int((end_time - start_time).total_seconds() / 60)
                    
                    # Create a semicolon-separated line ready for COPY
                    wkt_point = f"SRID=4326;POINT({longitude} {latitude})"
                    
                    outfile.write(f'"{stop_id}";"{address}";"{wkt_point}";"{start_time}";"{end_time}";{duration}\n')
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
        copy_command = f"""\\COPY month_{month:02d}_stops (stop_id, address, location, start_time, end_time, duration_minutes) 
                           FROM '{processed_file}' WITH (FORMAT csv, DELIMITER E';', QUOTE '"', ESCAPE '\\', NULL '\\N')"""
        
        cmd = [
            "psql",
            "-h", conn_params['host'],
            "-p", str(conn_params['port']),
            "-d", conn_params['dbname'],
            "-U", conn_params['user'],
            "-c", copy_command
        ]
        
        env = os.environ.copy()
        env["PGPASSWORD"] = conn_params['password']
        
        start_time = time.time()
        print(f"Running COPY command for Worker {worker_id}")
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        print(f"DONE running COPY command for Worker {worker_id}")
        
        if result.returncode != 0:
            print(f"Worker {worker_id} error: {result.stderr}")
            return 0
        
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
    """Create indexes after data is loaded"""
    conn_string = f"host={conn_params['host']} port={conn_params['port']} dbname={conn_params['dbname']} user={conn_params['user']} password={conn_params['password']}"
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cursor = conn.cursor()
    
    print("Creating indexes (this may take a few minutes)...")
    
    cursor.execute(f"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_stop_id_{month:02d}') THEN
            CREATE INDEX idx_stop_id_{month:02d} ON month_{month:02d}_stops(stop_id);
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_start_time_{month:02d}') THEN
            CREATE INDEX idx_start_time_{month:02d} ON month_{month:02d}_stops(start_time);
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_end_time_{month:02d}') THEN
            CREATE INDEX idx_end_time_{month:02d} ON month_{month:02d}_stops(end_time);
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_location_{month:02d}') THEN
            CREATE INDEX idx_location_{month:02d} ON month_{month:02d}_stops USING GIST(location);
        END IF;
    END $$;
    """)
    
    cursor.close()
    conn.close()
    print("Indexes created successfully.")

def main():
    parser = argparse.ArgumentParser(description='Parallel data loader for stop data into PostGIS')
    parser.add_argument('--month', type=int, required=True, help='Month number (1-12)')
    parser.add_argument('--host', type=str, default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--dbname', type=str, default='mydatabase', help='Database name')
    parser.add_argument('--user', type=str, default='postgres', help='Database user')
    parser.add_argument('--password', type=str, required=True, help='Database password')
    parser.add_argument('--workers', type=int, default=0, 
                        help='Number of parallel workers (0=auto based on CPU count)')
    
    args = parser.parse_args()
    
    workers = args.workers if args.workers > 0 else os.cpu_count()
    
    conn_params = {
        'host': args.host,
        'port': args.port,
        'dbname': args.dbname,
        'user': args.user,
        'password': args.password
    }
    
    file_path = os.path.join("monthly_stop_data", f"month_{args.month:02d}.csv")
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} does not exist")
        return
    
    setup_database(conn_params, args.month)
    
    print(f"Splitting file into {workers} chunks...")
    chunk_files = split_file_into_chunks(file_path, workers)
    
    print(f"Starting parallel load with {workers} workers...")
    start_time = time.time()
    total_rows = 0
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        futures = []
        for i, chunk_file in enumerate(chunk_files):
            future = executor.submit(load_chunk, chunk_file, conn_params, i+1, args.month)
            futures.append(future)
        
        for future in concurrent.futures.as_completed(futures):
            rows = future.result()
            total_rows += rows
    
    total_time = time.time() - start_time
    avg_rate = total_rows / total_time if total_time > 0 else 0
    
    print(f"\nLoading complete!")
    print(f"Total rows: {total_rows:,}")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Average rate: {avg_rate:.2f} rows/second")
    
    create_indexes(conn_params, args.month)

if __name__ == "__main__":
    main() 