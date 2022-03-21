------------------------------------------------------------------------------------------------------------------------------------------------------
-- Cluster Management
------------------------------------------------------------------------------------------------------------------------------------------------------
-- Check the active worker nodes:
SELECT * FROM citus_get_active_worker_nodes();

-- Create the MobilityDB extension on the workers:
SELECT run_command_on_workers($cmd$ CREATE EXTENSION mobilitydb CASCADE; $cmd$);

-- Create a distriubted table
CREATE TABLE trips_dist (LIKE trips);
SELECT create_distributed_table('trips_dist', 'vehicle');

-- Insert Data into the distributed table
INSERT INTO trips_dist
SELECT * FROM trips;

-- Create the necessary indexes
CREATE INDEX trips_dist_trip_gist_idx on trips_dist USING gist(trip);
CREATE INDEX trips_dist_trip_spgist_idx on trips_dist USING spgist(trip);

-- Create the referrence tables
CREATE TABLE municipalities_ref (LIKE municipalities);

INSERT INTO municipalities_ref SELECT * FROM municipalities;
SELECT create_reference_table('municipalities_ref');

CREATE TABLE poi_ref (LIKE poi INCLUDING ALL);
INSERT INTO poi_ref SELECT * FROM poi;
SELECT create_reference_table('poi_ref');

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of trips and the total number of points per trip:
------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT count(*) numTrajs, sum(numinstants(trip)) numPoints 
FROM trips_dist;

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Temporal Range Query: Which vehicle trips took place during a specific time frame?
------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT distinct vehicle 
FROM trips_dist
WHERE trip && period ('2020-06-03 20:00', '2020-06-03 20:30');

--Query Plan:
EXPLAIN 
SELECT distinct vehicle 
FROM trips_dist
WHERE trip && period ('2020-06-03 20:00', '2020-06-03 20:30');

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Spatial Range Query: Which vehicle trips passed in the municipality of Evere
------------------------------------------------------------------------------------------------------------------------------------------------------   
SELECT distinct t.vehicle
FROM trips_dist t, municipalities_ref m
WHERE m.name like '%Evere%'
   AND intersects(t.trip, m.geom);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Spatiotemporal Range Query: Which vehicle trips passed in the municipality of Evere during a specific period.
------------------------------------------------------------------------------------------------------------------------------------------------------   
SELECT distinct t.vehicle
FROM trips_dist t, municipalities_ref m
WHERE m.name like '%Evere%'
   AND t.trip && period ('2020-06-03 20:00', '2020-06-03 20:30')
   AND intersects(atPeriod(t.trip, period ('2020-06-03 20:00', '2020-06-03 20:30')) , m.geom);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Broadcast-Join Query: For each municipality in Brussels, give the number of trips that have passed through each of them.
------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT m.name, count(*)
FROM trips_dist t, municipalities_ref m
WHERE intersects(t.trip, m.geom);

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Spatial kNN Query: Find the ten nearest vehicles to the Grand Place of Brussels
------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT t.vehicle
FROM trips_dist t, poi_ref p
WHERE p.name = 'Grand Place - Grote Markt'
ORDER BY (t.trip |=| p.geom) asc
LIMIT 10;

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Spatiotemporal join Queries (Not supported): Find the pairs of trips that move close with respect to a distance 50 meters
----------------------------------------------------------------------------------------------------------------------------------------------------

SELECT t1.vehicle, t2.vehicle
FROM trips_dist t1, trips_dist t2
WHERE t1.vehicle < t2.vehicle
   AND dwithin(t1.trip, t2.trip, 50)
