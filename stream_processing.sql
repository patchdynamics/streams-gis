-- create table to hold all points of polylines plus gids
-- this dataset appears to have exact overlap between adjacent polylines
create sequence reach_points_sequence cycle;
drop table if exists reach_points;
create table reach_points as select nextval('stream_points_sequence'::regclass) as point_id, gid, (ST_DumpPoints(geom)).geom, (st_dumppoints(geom)).path[2],  ST_AsText((ST_DumpPoints(geom)).geom) from hubbardnhd;
create index reach_points_index ON stream_points using GIST (geom);

-- find all points that are contained in two reaches
-- ie all the connecting points
drop table if exists reach_connections;
create table reach_connections as select s1.point_id, s1.gid s1_gid, s1.path, s2.gid s2_gid from reach_points s1,  reach_points s2 where st_equals(s1.geom, s2.geom) and s1.gid != s2.gid;

-- reach connetions contains all connections, bidirectionally joined
-- what we want is a table of these nodes, and a table of which reaches are connected to them
drop table if exists reach_nodes;
create sequence reach_nodes_sequence cycle;
create table reach_nodes as select nextval('reach_nodes_sequence'::regclass) as id, ST_POINT(ST_X(geom), ST_Y(geom)) as node from (select distinct geom from  reach_connections join reach_points on reach_connections.point_id = reach_points.point_id ) reach_connections_geom;

-- and now use the fact that each node is a spatial 'bucket' to get all the reaches connected to each node
create sequence reach_adjacencies_sequence cycle;
drop table if exists reach_adjacencies;
create table reach_adjacencies as select nextval('reach_adjacencies_sequence'), gid, node_id from (select distinct gid, reach_nodes.id as node_id from reach_nodes, reach_points where st_equals(reach_nodes.node, reach_points.geom)) points;

-- then find reaches that only have a single connected point
-- these are the first order ones

alter table hubbardnhd add column stream_order int;
update hubbardnhd set stream_order = -1;
update hubbardnhd set stream_order = 1 from ( select gid from reach_adjacencies group by gid having count(gid) = 1 ) zero_order where zero_order.gid = hubbardnhd.gid;

-- and now data is ready for the standard stahler algorithm
-- this algorithm could possibly be implemented within postgis as a function
-- but we'll handle it in python for now
-- do we need to flip it back to a graph where the reachs are the nodes ?
-- right now we have to go through 3 tables, should only be 2..


-- what we REALLY need are the zero order nodes


-- for fun, here's playing around in the db
alter table reach_nodes add column node_order int;
update reach_nodes set node_order = -1;

update reach_nodes set node_order = 1 from (
select reach_adjacencies.node_id from reach_adjacencies join ( select node_id from reach_adjacencies r_a join hubbardnhd h on h.gid = r_a.gid where h.stream_order = 1 ) nodes_with_first_order on reach_adjacencies.node_id = nodes_with_first_order.node_id group by reach_adjacencies.node_id having count(reach_adjacencies.node_id) = 2
) first_order where first_order.node_id = reach_nodes.id;

