# Gets all points inside Utah
all_points_inside_utah = """
SELECT *
FROM month_01_routes
WHERE ST_Within(
    location::geometry,
    ST_MakeEnvelope(-114.064453, 37.026061, -109.054687, 42.008507, 4326)
);
"""

# Gets all points outside Utah
all_points_outside_utah = """
SELECT *
FROM month_01_routes 
    WHERE NOT ST_Within(
        location::geometry,
        ST_MakeEnvelope(-114.064453, 37.026061, -109.054687, 42.008507, 4326)
    );
"""

# This gets all truck_ids where the first point is inside Utah and the last point is outside Utah
from_utah_trucks = """
WITH truck_routes AS (
    SELECT
        truck_id,
        FIRST_VALUE(location::geometry) OVER (PARTITION BY truck_id ORDER BY timestamp) AS start_location,
        FIRST_VALUE(location::geometry) OVER (PARTITION BY truck_id ORDER BY timestamp DESC) AS end_location
    FROM month_01_routes
)
SELECT DISTINCT truck_id
FROM truck_routes
WHERE ST_Within(start_location, ST_MakeEnvelope(-114.064453, 37.026061, -109.054687, 42.008507, 4326))
  AND NOT ST_Within(end_location, ST_MakeEnvelope(-114.064453, 37.026061, -109.054687, 42.008507, 4326));
"""

# This gets all truck_ids where the first point is outside Utah and the last point is inside Utah
to_utah_trucks = """
WITH truck_routes AS (
    SELECT
        truck_id,
        FIRST_VALUE(location::geometry) OVER (PARTITION BY truck_id ORDER BY timestamp) AS start_location,
        FIRST_VALUE(location::geometry) OVER (PARTITION BY truck_id ORDER BY timestamp DESC) AS end_location
    FROM month_01_routes
    )
    SELECT DISTINCT truck_id
    FROM truck_routes
    WHERE NOT ST_Within(start_location, ST_MakeEnvelope(-114.064453, 37.026061, -109.054687, 42.008507, 4326))
      AND ST_Within(end_location, ST_MakeEnvelope(-114.064453, 37.026061, -109.054687, 42.008507, 4326));
"""

# Create a table of all the points of a truck_id that started in Utah and ended outside Utah
create_to_utah_trucks_table = """
CREATE TABLE from_utah_trucks AS
SELECT *
FROM month_01_routes
WHERE truck_id IN (
    WITH truck_routes AS (
        SELECT
            truck_id,
            FIRST_VALUE(location::geometry) OVER (PARTITION BY truck_id ORDER BY timestamp) AS start_location,
            FIRST_VALUE(location::geometry) OVER (PARTITION BY truck_id ORDER BY timestamp DESC) AS end_location
        FROM truck_locations
    )
    SELECT DISTINCT truck_id
    FROM truck_routes
    WHERE ST_Within(start_location, ST_MakeEnvelope(-114.064453, 37.026061, -109.054687, 42.008507, 4326))
      AND NOT ST_Within(end_location, ST_MakeEnvelope(-114.064453, 37.026061, -109.054687, 42.008507, 4326))
);
"""

# Create a table of all the points of a truck_id that started outside Utah and ended inside Utah
create_from_utah_trucks_table = """
CREATE TABLE from_utah_trucks AS
SELECT *
FROM month_01_routes
WHERE truck_id IN (
    WITH truck_routes AS (
        SELECT
            truck_id,
            FIRST_VALUE(location::geometry) OVER (PARTITION BY truck_id ORDER BY timestamp) AS start_location,
            FIRST_VALUE(location::geometry) OVER (PARTITION BY truck_id ORDER BY timestamp DESC) AS end_location
        FROM truck_locations
    )
    SELECT DISTINCT truck_id
    FROM truck_routes
    WHERE ST_Within(end_location, ST_MakeEnvelope(-114.064453, 37.026061, -109.054687, 42.008507, 4326))
      AND NOT ST_Within(start_location, ST_MakeEnvelope(-114.064453, 37.026061, -109.054687, 42.008507, 4326))
);
"""

# create indexes for new tables
"CREATE INDEX idx_filtered_truck_points_truck_id ON <table name> (truck_id);"
"CREATE INDEX idx_filtered_truck_points_location ON <table name> USING GIST (location);"

"""
WITH qualifying_trucks AS (
    SELECT DISTINCT truck_id
    FROM (
        SELECT
            truck_id,
            FIRST_VALUE(location) OVER (PARTITION BY truck_id ORDER BY timestamp) AS start_location,
            FIRST_VALUE(location) OVER (PARTITION BY truck_id ORDER BY timestamp DESC) AS end_location,
            FIRST_VALUE(timestamp) OVER (PARTITION BY truck_id ORDER BY timestamp) AS first_timestamp
        FROM month_01_routes
    ) tr
    WHERE ST_Within(tr.start_location::geometry, ST_MakeEnvelope(-114.064453, 37.026061, -109.054687, 42.008507, 4326))
        AND NOT ST_Within(tr.end_location::geometry, ST_MakeEnvelope(-114.064453, 37.026061, -109.054687, 42.008507, 4326))
        AND tr.first_timestamp BETWEEN EXTRACT(EPOCH FROM '2023-01-01T00:00:00Z'::timestamp)::bigint 
                                AND EXTRACT(EPOCH FROM '2023-01-05T23:59:59Z'::timestamp)::bigint
)
SELECT r.*
FROM month_01_routes r
INNER JOIN qualifying_trucks qt ON r.truck_id = qt.truck_id
LIMIT 1000;
"""


"""
WITH route_segments AS (
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
    FROM month_01_routes
),
routes_numbered AS (
    SELECT 
        id,
        truck_id,
        timestamp,
        SUM(new_route_flag) OVER (PARTITION BY truck_id ORDER BY timestamp) AS route_num
    FROM route_segments
)
UPDATE month_01_routes t
SET route_id = rn.route_num
FROM routes_numbered rn
WHERE t.id = rn.id;
"""