#!/usr/bin/python
import psycopg2

junctions_table = "hubbard_net_junctions"
raster_table = "hubbard_watershed_dem"

conn = psycopg2.connect(database="streams", password="", host="127.0.0.1", port="5432")
cur = conn.cursor()

update = "update %s " % (junctions_table) + " junctions " \
					"SET elevation = ST_VALUE(rast, junctions.geom) " \
					"FROM  \"%s\" junctions2 CROSS JOIN \"%s\" raster " % (junctions_table, raster_table) + " " \
					"WHERE junctions.gid = junctions2.gid"
print update


