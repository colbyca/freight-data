from datetime import datetime
import os
import time
import psycopg2
import concurrent.futures
import subprocess
import argparse
import tempfile
import math

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate the great circle distance between two points on the earth"""
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 3956  # Radius of earth in miles
    return c * r

def calculate_speed(lat1, lon1, lat2, lon2, time_diff_seconds):
    """Calculate speed in miles per hour between two points"""
    if time_diff_seconds <= 0:
        return float('inf')
    
    distance = haversine_distance(lat1, lon1, lat2, lon2)
    hours = time_diff_seconds / 3600
    return distance / hours

def split_file_into_chunks(file_path, num_chunks):
    """Split a large file into chunks at route boundaries and return temp file paths"""
    # First pass: count total lines and find ideal split points with route boundaries
    total_lines = 0
    last_timestamps = {}   # Keep track of last timestamp for each truck
    
    print("First pass: counting lines...")
    with open(file_path, 'r') as f:
        for _ in f:
            total_lines += 1
    
    # Calculate ideal split points
    ideal_chunk_size = total_lines / num_chunks
    ideal_split_points = [int(i * ideal_chunk_size) for i in range(1, num_chunks)]
    actual_split_points = [1]  # Start with first line
    
    print(f"Total lines: {total_lines:,}")
    print(f"Ideal chunk size: {ideal_chunk_size:,.0f} lines")
    print("Finding nearest route boundaries to split points...")
    
    current_line = 0
    split_idx = 0
    
    with open(file_path, 'r') as f:
        while split_idx < len(ideal_split_points) and current_line < total_lines:
            current_line += 1
            line = f.readline()
            
            # If we're approaching an ideal split point, start looking for route boundaries
            if current_line >= ideal_split_points[split_idx]:
                try:
                    parts = line.strip().split(';')
                    if len(parts) >= 6:
                        truck_id = parts[0]
                        timestamp = int(parts[3])
                        
                        if truck_id in last_timestamps:
                            # Check if time gap is more than 6 hours (21600 seconds)
                            time_gap = timestamp - last_timestamps[truck_id]
                            if time_gap > 21600:
                                # Found a route boundary after the ideal split point
                                actual_split_points.append(current_line)
                                print(f"Split point {split_idx + 1}: Ideal={ideal_split_points[split_idx]:,}, Actual={current_line:,}")
                                split_idx += 1
                        
                        last_timestamps[truck_id] = timestamp
                except Exception as e:
                    print(f"Error processing line {current_line}: {str(e)}")
                    continue
            
            # Update last timestamps even when not near split point
            if current_line % 1000000 == 0:
                print(f"Processed {current_line:,} lines...")
            
            try:
                parts = line.strip().split(';')
                if len(parts) >= 6:
                    truck_id = parts[0]
                    timestamp = int(parts[3])
                    last_timestamps[truck_id] = timestamp
            except Exception:
                pass
    
    # Add the final boundary
    actual_split_points.append(total_lines + 1)
    
    # Create the chunks based on the calculated boundaries
    temp_files = []
    print("\nCreating chunk files...")
    
    for i in range(len(actual_split_points) - 1):
        start_line = actual_split_points[i]
        end_line = actual_split_points[i + 1] - 1
        
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv')
        temp_files.append(temp_file.name)
        
        lines_written = 0
        with open(file_path, 'r') as source, open(temp_file.name, 'w') as dest:
            # Skip to start line
            for _ in range(start_line - 1):
                next(source)
            
            # Write lines until end line
            for _ in range(end_line - start_line + 1):
                line = next(source)
                dest.write(line)
                lines_written += 1
        
        print(f"Created chunk {i+1} with {lines_written:,} lines (lines {start_line:,} to {end_line:,})")
    
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
        collection_date DATE NOT NULL,
        route_id INT
    );
    """)
    
    cursor.close()
    conn.close()
    
    print("Database schema ready.")

def prepare_temp_files_for_copy(input_file, output_file, worker_id):
    """Process input file to create a COPY-compatible format with transformed data"""
    # Dictionary to keep track of last timestamp for each truck
    last_timestamps = {}
    # Dictionary to keep track of last valid coordinates for each truck
    last_coordinates = {}
    # Dictionary to keep track of current route ID for each truck
    current_route_ids = {}
    # Dictionary to keep track of recent points for each truck
    recent_points = {}  # Will store up to 5 recent points per truck
    # Dictionary to track if we're in a stuck point sequence
    stuck_points = {}  # Maps truck_id to (count, coordinates)
    # Global route counter for this worker
    route_counter = worker_id * 1000000  # Ensure unique route IDs across workers
    
    def is_point_stuck(current_point, recent_points_list):
        """Check if a point is stuck at the same coordinates"""
        if not recent_points_list:
            return False
            
        # Check if current point is at same coordinates as previous points
        current_coords = (current_point[0], current_point[1])
        for prev_point in recent_points_list:
            prev_coords = (prev_point[0], prev_point[1])
            if abs(current_coords[0] - prev_coords[0]) > 0.0001 or abs(current_coords[1] - prev_coords[1]) > 0.0001:
                return False
        return True
    
    def is_point_valid(current_point, recent_points_list):
        """Determine if a point is valid based on its relationship to recent points"""
        if not recent_points_list:
            return True  # First point is always included
            
        # Calculate speeds to all recent points
        speeds = []
        for prev_point in recent_points_list:
            time_diff = current_point[2] - prev_point[2]
            if time_diff <= 3600:  # Only consider points within 1 hour
                speed = calculate_speed(prev_point[0], prev_point[1], 
                                      current_point[0], current_point[1], 
                                      time_diff)
                speeds.append(speed)
        
        if not speeds:
            return True  # No recent points within time window
            
        # If we have at least 2 recent points, use median speed
        if len(speeds) >= 2:
            median_speed = sorted(speeds)[len(speeds)//2]
            return median_speed <= 200  # Filter if median speed is too high
        
        # If only one recent point, be more lenient
        return speeds[0] <= 300  # Higher threshold for single point comparison
    
    def should_start_new_route(truck_id, current_point, last_point):
        """Determine if we should start a new route based on time and distance"""
        if not last_point:
            return True
            
        time_diff = current_point[2] - last_point[2]
        distance = haversine_distance(last_point[0], last_point[1], 
                                    current_point[0], current_point[1])
        
        # Start new route if:
        # 1. Time gap is more than 6 hours
        # 2. OR if time gap is more than 1 hour AND distance is more than 100 miles
        # 3. OR if we've been stuck at the same coordinates for 3 or more points
        if time_diff > 21600:  # 6 hours
            return True
        elif time_diff > 3600 and distance > 100:  # 1 hour and 100 miles
            return True
        elif stuck_points.get(truck_id, (0, None))[0] >= 3:
            return True
            
        return False
    
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for i, line in enumerate(infile):
            try:
                parts = line.strip().split(';')
                if len(parts) >= 6:
                    truck_id = parts[0]
                    latitude = float(parts[1])
                    longitude = float(parts[2])
                    timestamp = int(parts[3])
                    speed = parts[4]
                    is_valid = parts[5] == '1'
                    
                    # Initialize data structures if needed
                    if truck_id not in recent_points:
                        recent_points[truck_id] = []
                        stuck_points[truck_id] = (0, None)
                    
                    current_point = (latitude, longitude, timestamp)
                    current_coords = (latitude, longitude)
                    
                    # Check if we're in a stuck point sequence
                    is_stuck = is_point_stuck(current_point, recent_points[truck_id])
                    if is_stuck:
                        count, coords = stuck_points[truck_id]
                        if coords == current_coords:
                            stuck_points[truck_id] = (count + 1, coords)
                        else:
                            stuck_points[truck_id] = (1, current_coords)
                    else:
                        stuck_points[truck_id] = (0, None)
                    
                    # Get last valid point for this truck
                    last_point = last_coordinates.get(truck_id)
                    
                    # Check if we need to start a new route
                    new_route = should_start_new_route(truck_id, current_point, last_point)
                    
                    # Clear recent points when starting a new route
                    if new_route:
                        recent_points[truck_id] = []
                        stuck_points[truck_id] = (0, None)
                        if worker_id == 1 and i % 100000 == 0:
                            print(f"Starting new route at {latitude}, {longitude}")
                    
                    # Check if point is valid
                    should_include = is_point_valid(current_point, recent_points[truck_id])
                    
                    # Update recent points if this point is valid
                    if should_include:
                        recent_points[truck_id].append(current_point)
                        # Keep only the 5 most recent points
                        if len(recent_points[truck_id]) > 5:
                            recent_points[truck_id].pop(0)
                        # Update last valid coordinates
                        last_coordinates[truck_id] = current_point
                    
                    # Assign or increment route ID
                    if new_route:
                        route_counter += 1
                        current_route_ids[truck_id] = route_counter
                    
                    # Update last timestamp for this truck
                    last_timestamps[truck_id] = timestamp
                    
                    # Get current route ID for this truck
                    route_id = current_route_ids.get(truck_id, route_counter)
                    
                    # Only write the point if it passed our filters
                    if should_include:
                        # Create a tab-separated line ready for COPY
                        # Format: truck_id, WKT point, timestamp, speed, is_valid, collection_date, route_id
                        wkt_point = f"SRID=4326;POINT({longitude} {latitude})"
                        collection_date = datetime.fromtimestamp(timestamp).date()
                        
                        outfile.write(f"{truck_id},{wkt_point},{timestamp},{speed},{is_valid},{collection_date},{route_id}\n")
                        if i % 100000 == 0 and worker_id == 1:
                            print(f"Processed {i} lines...")
                    elif worker_id == 1 and i % 100000 == 0:
                        print(f"Filtered point at {latitude}, {longitude}")

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
        copy_command = f"""\\COPY month_{month:02d}_routes (truck_id, location, timestamp, speed, is_valid, collection_date, route_id) 
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

def process_route_chunk(chunk_file, conn_params, worker_id, month):
    """Process a chunk of data to create route IDs"""
    try:
        # Create temporary table for this chunk's route detection
        conn_string = f"host={conn_params['host']} port={conn_params['port']} dbname={conn_params['dbname']} user={conn_params['user']} password={conn_params['password']}"
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Create temporary table for this chunk
        cursor.execute(f"""
        CREATE TEMP TABLE temp_routes_chunk_{worker_id} AS 
        SELECT 
            id,
            truck_id,
            timestamp,
            CASE 
                WHEN TO_TIMESTAMP(timestamp) - 
                     TO_TIMESTAMP(LAG(timestamp) OVER (PARTITION BY truck_id ORDER BY timestamp)) 
                     > INTERVAL '1 day' 
                THEN 1 
                ELSE 0 
            END AS new_route_flag
        FROM month_{month:02d}_routes
        WHERE id IN (
            SELECT id FROM month_{month:02d}_routes
            ORDER BY id
            LIMIT 1000000
            OFFSET {worker_id * 1000000}
        );
        """)
        
        # Create temporary table for route numbering
        cursor.execute(f"""
        CREATE TEMP TABLE temp_routes_numbered_chunk_{worker_id} AS
        SELECT 
            id,
            truck_id,
            timestamp,
            SUM(new_route_flag) OVER (PARTITION BY truck_id ORDER BY timestamp) AS route_num
        FROM temp_routes_chunk_{worker_id};
        """)
        
        # Update the main table with route IDs for this chunk
        cursor.execute(f"""
        UPDATE month_{month:02d}_routes t
        SET route_id = trn.route_num
        FROM temp_routes_numbered_chunk_{worker_id} trn
        WHERE t.id = trn.id
        AND t.route_id IS NULL;
        """)
        
        # Clean up temporary tables
        cursor.execute(f"""
        DROP TABLE IF EXISTS temp_routes_chunk_{worker_id};
        DROP TABLE IF EXISTS temp_routes_numbered_chunk_{worker_id};
        """)
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Worker {worker_id} exception during route processing: {str(e)}")
        return False

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
        
        IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_route_id') THEN
            CREATE INDEX idx_route_id ON month_{month:02d}_routes(route_id);
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