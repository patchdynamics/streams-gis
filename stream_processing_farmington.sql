-- farmington
drop table if exists reach_points;
create table reach_points as select nextval('stream_points_sequence'::regclass) as point_id, gid, (ST_DumpPoints(prepared_geom)).geom, (st_dumppoints(geom)).path[2],  ST_AsText((ST_DumpPoints(prepared_geom)).geom) from farmington_streams;
create index reach_points_index ON stream_points using GIST (geom);
alter table reach_points add column point GEOMETRY;
-- removes the z and m coordinates

-- POINT(-73.1879261301711 42.1119012679671 0 0) - streams
-- POINT(-73.187926130171  42.1119012679672) - hydro net
-- slight precision error between these 2 datasets, fantastic!
-- here's the fix
update reach_points set point = ST_SnapToGrid(ST_POINT(ST_X(geom), ST_Y(geom)), 0.00000000001)


-- and now use the fact that each node is a spatial 'bucket' to get all the reaches connected to each node
create sequence reach_adjacencies_sequence cycle;

-- for the farmington

alter table "farmington hydro net junctions" add column prepared_geom Geometry(Point); 
alter table farmington_streams add column prepared_geom Geometry(MultiLineString);

-- should the grid should get iteratively smaller to bin the nodes with the streams
update "farmington hydro net junctions" set prepared_geom = ST_SnapToGrid(ST_Force2d(geom), 0.00000001);
update farmington_streams set prepared_geom = ST_SnapToGrid(ST_Force2d(geom), 0.00000001);

drop table if exists reach_adjacencies;
create table reach_adjacencies as select nextval('reach_adjacencies_sequence'), stream_id, node_id 
from (
	select distinct streams.gid as stream_id, junctions.gid as node_id 
	from farmington_streams streams, "farmington hydro net junctions" junctions
	where st_covers(streams.prepared_geom, junctions.prepared_geom)
) segment_junctions;

-- should not return any rows
select count(node_id), stream_id from reach_adjacencies group by stream_id having count(node_id) = 1;
