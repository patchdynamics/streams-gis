select junctions1.gid, junctions1.node_order, count(ra1.nextval)
from reach_adjacencies ra1, reach_adjacencies ra2, 
"farmington_hydro_net_junctions" junctions1, "farmington_hydro_net_junctions" junctions2
where ra1.stream_id = ra2.stream_id
and junctions1.gid = ra1.node_id
and junctions2.gid = ra2.node_id
and junctions1.elevation >= junctions2.elevation
group by junctions1.gid, junctions1.node_order

select junctions1.gid, junctions1.elevation, junctions2.gid,junctions2.elevation 
 from reach_adjacencies ra1, reach_adjacencies ra2, 
farmington_hydro_net_junctions junctions1 , farmington_hydro_net_junctions junctions2 
where ra1.stream_id = ra2.stream_id 
and junctions1.gid = ra1.node_id
and junctions2.gid = ra2.node_id 
and junctions1.gid != junctions2.gid
and junctions1.gid = 27

select ra2.node_id, ra1.stream_id from reach_adjacencies ra1, reach_adjacencies ra2, farmington_hydro_net_junctions junctions1 , farmington_hydro_net_junctions junctions2 where ra1.stream_id = ra2.stream_id and junctions1.gid = ra1.node_id and junctions2.gid = ra2.node_id and ra1.node_id = 2724 and junctions2.node_order = -1
