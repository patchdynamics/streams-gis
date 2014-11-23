-- and now use the fact that each node is a spatial 'bucket' to get all the reaches connected to each node
create sequence reach_adjacencies_sequence cycle;
create sequence base_stream_id_sequences cycle;

-- for the farmington

alter table "passumpsichydronetjunctions" add column prepared_geom Geometry(Point); 
alter table passumpsichigh add column prepared_geom Geometry(MultiLineString);
alter table "passumpsichydronetjunctions" add column base_stream_id integer;
alter table passumpsichigh add column base_stream_id integer;

-- should the grid should get iteratively smaller to bin the nodes with the streams
update "passumpsichydronetjunctions" set prepared_geom = ST_SnapToGrid(ST_Force2d(geom), 0.00000001);
update passumpsichigh set prepared_geom = ST_SnapToGrid(ST_Force2d(geom), 0.00000001);

drop table if exists reach_adjacencies;
create table reach_adjacencies as select nextval('reach_adjacencies_sequence'), stream_id, node_id 
from (
	select distinct streams.gid as stream_id, junctions.gid as node_id 
	from passumpsichigh streams, "passumpsichydronetjunctions" junctions
	where st_covers(streams.prepared_geom, junctions.prepared_geom)
	and junctions.enabled = 1
) segment_junctions;

-- should not return any rows
select count(node_id), stream_id from reach_adjacencies group by stream_id having count(node_id) = 1;

-- consolidating to base stream
create sequence base_stream_id_sequences cycle;
