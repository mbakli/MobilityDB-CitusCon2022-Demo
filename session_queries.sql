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
CREATE TABLE communes_ref (LIKE communes);
INSERT INTO communes_ref SELECT * FROM communes;
SELECT create_reference_table('communes_ref');

CREATE TABLE Querypoints_ref (LIKE Querypoints INCLUDING ALL);
INSERT INTO Querypoints_ref SELECT * FROM Querypoints;
SELECT create_reference_table('Querypoints_ref');

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Partitions Summary
------------------------------------------------------------------------------------------------------------------------------------------------------
--Query Text: Total number of trips per partition

--SQL:
SELECT *
FROM run_command_on_shards('trips_dist', $cmd$
   SELECT count(*) trips_count FROM %1$s
$cmd$); 


--Query Text: Total number of instants per partition

--SQL:
SELECT *
FROM run_command_on_shards('trips_dist', $cmd$
   SELECT sum(numinstants(trip)) Instants FROM %1$s
$cmd$);  

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of trips and the total number of points per trip:
------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT count(*) numTrajs, sum(numinstants(trip)) numPoints 
FROM trips_dist;

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Temporal Range Query: Find trips that overlap a given query range.
------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT distinct vehicle 
FROM trips_dist
WHERE trip && period '[2020-06-03, 2020-06-05)';

--Query Plan:
EXPLAIN 
SELECT distinct vehicle 
FROM trips_dist
WHERE trip && period '[2020-06-03, 2020-06-05)';

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Spatial Range Query: Find trips that overlap a given region.
------------------------------------------------------------------------------------------------------------------------------------------------------   
SELECT distinct vehicle
FROM trips_dist
WHERE intersects(trip, 'SRID=3857;POLYGON((481332.4856234445 6586813.81152605,481332.4856234445 6588687.81152605,483206.4856234445 6588687.81152605,483206.4856234445 6586813.81152605,481332.4856234445 6586813.81152605))'::geometry);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Spatiotemporal Range Query: Find trips that overlap a given region during a specific period.
------------------------------------------------------------------------------------------------------------------------------------------------------   
SELECT distinct vehicle
FROM trips_dist
WHERE intersects(trip, 'SRID=3857;POLYGON((481332.4856234445 6586813.81152605,481332.4856234445 6588687.81152605,483206.4856234445 6588687.81152605,483206.4856234445 6586813.81152605,481332.4856234445 6586813.81152605))'::geometry);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Broadcast-Join Query
------------------------------------------------------------------------------------------------------------------------------------------------------SELECT DISTINCT P.Id, P.geom
FROM Trips_dist T, (SELECT * FROM Querypoints_ref  order by id desc LIMIT 5) P
WHERE Intersects(T.Trip, P.geom) 
ORDER BY P.Id

SELECT distinct c.id, t.vehicle
FROM trips_dist t, queryperiods_ref c
WHERE t.trip && c.period

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Spatial kNN Query
------------------------------------------------------------------------------------------------------------------------------------------------------
--kNN Query:
SELECT t.vehicle, t.day, t.seq, (trip |=| way) AS distance
FROM trips_dist t, planet_osm_point_ref r
WHERE name = 'Grand Place - Grote Markt'
ORDER BY distance asc
LIMIT 10;

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Spatiotemporal join Queries (Not supported)
----------------------------------------------------------------------------------------------------------------------------------------------------

