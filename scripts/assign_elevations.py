#!/usr/bin/python
import psycopg2

junctions_table = "passumpsic_junctions_fixed"
raster_table = "passumpsicelevationswgs84"

conn = psycopg2.connect(database="streams", password="", host="127.0.0.1", port="5432")
cur = conn.cursor()

update = "update %s " % (junctions_table) + " junctions " \
					"SET elevation = ST_VALUE(rast, junctions.geom) " \
					"FROM  \"%s\" junctions2 CROSS JOIN \"%s\" raster " % (junctions_table, raster_table) + " " \
					"WHERE junctions.gid = junctions2.gid"
cur.execute(update)
conn.commit()


