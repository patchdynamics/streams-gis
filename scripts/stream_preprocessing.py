#!/usr/bin/python
print 'Gonna try to preprocess this..!'
import psycopg2

junctions_table = "passumpsic_junctions_fixed"
streams_table = "passumpsichigh"

conn = psycopg2.connect(database="streams", password="", host="127.0.0.1", port="5432")
cur = conn.cursor()

if 0:
	cur.execute("alter table \"%s\" add column prepared_geom Geometry(Point)" % (junctions_table))
	cur.execute("alter table \"%s\" add column node_order integer" % (junctions_table))
	cur.execute("alter table \"%s\" add column elevation decimal" % (junctions_table))
if 0:
	cur.execute("alter table \"%s\" add column prepared_geom Geometry(MultiLineString)" % (streams_table))
	cur.execute("alter table \"%s\" add column base_stream_id integer" % (streams_table))
	cur.execute("alter table \"%s\" add column stream_order integer" % (streams_table))

cur.execute("update \"%s\" set prepared_geom = ST_SnapToGrid(ST_Force2d(geom), 0.000001)" % (junctions_table))
cur.execute("update \"%s\" set prepared_geom = ST_SnapToGrid(ST_Force2d(geom), 0.000001)" % (streams_table))
#cur.execute("update \"%s\" set prepared_geom = ST_Force2d(geom)"  % (junctions_table))
#cur.execute("update \"%s\" set prepared_geom = ST_Force2d(geom)"  % (streams_table))

cur.execute("drop table if exists reach_adjacencies")
cur.execute("create table reach_adjacencies as select nextval('reach_adjacencies_sequence'), stream_id, node_id " 
		"from ( "
		" select distinct streams.gid as stream_id, junctions.gid as node_id "
		" from \"%s\" streams, \"%s\" junctions "
		" where st_covers(streams.prepared_geom, junctions.prepared_geom) "
		" and junctions.enabled != 0 "
		" and streams.enabled != 0 "
		") segment_junctions " % (streams_table, junctions_table) )


conn.commit()

print "done"
