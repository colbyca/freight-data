CREATE INDEX IF NOT EXISTS idx_truck_time ON month_01_routes (truck_id, timestamp);

ALTER TABLE month_01_routes ADD COLUMN IF NOT EXISTS route_id INT;


DROP TABLE IF EXISTS temp_routes;
CREATE TEMP TABLE temp_routes AS 
SELECT 
    id,
    truck_id,
    timestamp,
    -- Detect a new route when the time gap is greater than 1 day
    CASE 
        WHEN TO_TIMESTAMP(timestamp) - 
             TO_TIMESTAMP(LAG(timestamp) OVER (PARTITION BY truck_id ORDER BY timestamp)) 
             > INTERVAL '1 day' 
        THEN 1 
        ELSE 0 
    END AS new_route_flag
FROM month_01_routes;


DROP TABLE IF EXISTS temp_routes_numbered;
CREATE TEMP TABLE temp_routes_numbered AS
SELECT 
    id,
    truck_id,
    timestamp,
    -- Assign a cumulative route number per truck_id
    SUM(new_route_flag) OVER (PARTITION BY truck_id ORDER BY timestamp) AS route_num
FROM temp_routes;


UPDATE month_01_routes t
SET route_id = trn.route_num
FROM temp_routes_numbered trn
WHERE t.id = trn.id
AND t.route_id IS NULL;


-- UPDATE 5588391
-- Time: 1251959.881 ms (20:51.960)