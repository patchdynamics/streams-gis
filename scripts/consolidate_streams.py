#!/usr/bin/python
print 'Here we go a consolidatin\'!'
import psycopg2

junctions_table = "farmington_hydro_net_junctions"
streams_table = "farmington_streams"

class Stream():
	def __init__(self):
		self.id = -1


conn = psycopg2.connect(database="streams", password="", host="127.0.0.1", port="5432")
cur = conn.cursor()

cur.execute("update \"" + streams_table +"\" set base_stream_id = -1");

cur.execute("select gid from " + streams_table)

unprocessed = dict()

# load all streams into unprocessed bucket
row = cur.fetchone()
while row != None:
	stream = Stream()
	stream.id = row[0]
	unprocessed[stream.id] = stream
	row = cur.fetchone()


while True:
	stream = unprocessed.itervalues().next()
	print stream.id
	if stream == None:
		break


	# give this stream a base stream id
	cur.execute("select nextval('base_stream_id_sequences') ")
	base_stream_id = cur.fetchone()

	# place in bucket
	bucket = list()
	bucket.append(stream.id)

	while len(bucket) > 0:
		print bucket
		stream_id = bucket.pop(0)
		# update stream
		cur.execute("update " + streams_table + " set base_stream_id = %s where gid = %s ", [base_stream_id, stream_id]) 
		del unprocessed[stream_id]
	
		# iteratively give this base stream id to all segements connected to this segement that have only 2 connected streams
		query =	"select stream_id from reach_adjacencies, ( " \
							"select ra2.node_id, count(ra2.stream_id) stream_count " \
							"from reach_adjacencies ra, reach_adjacencies ra2 " \
							"where ra2.node_id = ra.node_id " \
							"and ra.stream_id = %s " \
							"and ra2.stream_id != %s " \
							"group by ra2.node_id " \
						" ) outlets " \
						" where reach_adjacencies.node_id = outlets.node_id " \
						" and reach_adjacencies.stream_id != %s" \ # here we need to not double back on streams that are already assigned.  may need to join streams table
						" and outlets.stream_count = 1 "
		print query
		cur.execute(query, [stream_id, stream_id, stream_id])
		row = cur.fetchone()
		while row != None:
			bucket.append(row[0])
			row = cur.fetchone()

