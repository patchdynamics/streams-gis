select yt1.*
from yourtable yt1
left outer join yourtable yt2
on (yt1.id = yt2.id and yt1.rev < yt2.rev)
where yt2.id is null;

select t1.*
from (select stream_id, elevation, node_id from reach_adjacencies ra, hubbard_net_junctions s where ra.stream_id = s.gid ) t1
left outer join (select stream_id, elevation, node_id from reach_adjacencies ra, hubbard_net_junctions s where ra.stream_id = s.gid ) t2
on (t1.stream_id = t2.stream_id and	t1.elevation  > t2.elevation)
where t2.stream_id is null;

update hubbard_net_junctions junctions
set endpoint = 0;

update hubbard_net_junctions junctions
set endpoint = 1
from (select base_stream_id, elevation, node_id from reach_adjacencies ra, hubbard_net_junctions j, hubbardnhd s where ra.stream_id = s.gid and ra.node_id = j.gid  ) t1
left outer join (select base_stream_id, elevation, node_id from reach_adjacencies ra, hubbard_net_junctions j, hubbardnhd s  where ra.stream_id = s.gid and ra.node_id = j.gid   ) t2
on (t1.base_stream_id = t2.base_stream_id and t1.elevation  > t2.elevation)
where t2.base_stream_id is null
and junctions.gid = t1.node_id;


update hubbard_net_junctions junctions
set endpoint = 1
from (select base_stream_id, elevation, node_id from reach_adjacencies ra, hubbard_net_junctions j, hubbardnhd s where ra.stream_id = s.gid and ra.node_id = j.gid  ) t1
left outer join (select base_stream_id, elevation, node_id from reach_adjacencies ra, hubbard_net_junctions j, hubbardnhd s  where ra.stream_id = s.gid and ra.node_id = j.gid   ) t2
on (t1.base_stream_id = t2.base_stream_id and t1.elevation  < t2.elevation)
where t2.base_stream_id is null
and junctions.gid = t1.node_id;
