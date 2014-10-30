-- consolidate the segements into reaches as defined by Jenn
drop table if exists consolidated_streams;
create table consolidated_streams as
select ST_CollectionHomogenize(ST_Collect(geom)) geom, base_stream_id
from (
	select ST_Force2D(geom) geom, base_stream_id 
	from farmington_streams where base_stream_id > -1
) streams  
group by base_stream_id;

-- add some more columns
alter table consolidated_streams add column min_elevation decimal;
alter table consolidated_streams add column max_elevation decimal;
alter table consolidated_streams add column length decimal;
alter table consolidated_streams add column slope decimal;

-- update consolidated streams with their min and max elevations
update consolidated_streams
set min_elevation=min, max_elevation=max, length=sum
from( 
	select min(elevation) min, max(elevation) max, sum(lengthkm)*1000 sum, farmington_streams.base_stream_id
	from farmington_streams, reach_adjacencies, "farmington hydro net junctions" junctions
	where reach_adjacencies.stream_id = farmington_streams.gid
	and reach_adjacencies.node_id = junctions.gid
	group by farmington_streams.base_stream_id
) min_max
where min_max.base_stream_id = consolidated_streams.base_stream_id;

