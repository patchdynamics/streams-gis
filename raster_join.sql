SELECT rid, ST_Value(rast, foo.geom) raster_val
FROM imgn42w073_1 CROSS JOIN "farmington hydro net junctions" As foo
WHERE foo.gid = 4702 limit 1;

UPDATE "farmington hydro net junctions" junctions
SET elevation = ST_VALUE(rast, j2.geom)
FROM  "farmington watershed dem" CROSS JOIN "farmington hydro net junctions" j2
WHERE j2.gid = junctions.gid;

# spot check
# 484.298553466797
