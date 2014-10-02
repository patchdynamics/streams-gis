-- create table stream_points as select gid, (ST_DumpPoints(geom)).geom,  ST_AsText((ST_DumpPoints(geom)).geom) from hubbardnhd where gid = 106 OR gid = 94;

-- create table to hold all points of polylines plus gids
-- this dataset appears to have exact overlap between adjacent polylines
CREATE SEQUENCE stream_points_sequence CYCLE;
drop table if exists stream_points;
create table stream_points as select nextval('stream_points_sequence'::regclass) as point_id, gid, (ST_DumpPoints(geom)).geom, (st_dumppoints(geom)).path[2],  ST_AsText((ST_DumpPoints(geom)).geom) from hubbardnhd;
create index stream_points_index ON stream_points using GIST (geom);

drop table if exists stream_adjacencies;
create table stream_adjacencies as select s1.point_id, s1.gid s1_gid, s1.path, s2.gid s2_gid from stream_points s1,  stream_points s2 where st_equals(s1.geom, s2.geom) and s1.gid != s2.gid;

-- then find reaches that only have a single connected point
-- these are the first order ones

select s1_gid, count(distinct point_id) from stream_adjacencies group by s1_gid having count(distinct point_id) = 1;

alter table hubbardnhd add column stream_order int;

-- locate the start of the first order streams
update hubbardnhd set stream_order = 1 from ( select s1_gid, count(distinct point_id) from stream_adjacencies group by s1_gid having count(distinct point_id) = 1 ) first_order where first_order.s1_gid = hubbardnhd.gid;


