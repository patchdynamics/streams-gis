#!/usr/bin/python
print 'Here we go!'
import psycopg2

junctions_table = "farmington_hydro_net_junctions"
streams_table = "farmington_streams"

class Node():
	def __init__(self):
		self.id = -1
		self.order = -1
		self.children_orders = []
		self.children_number = -1
		self.base_stream_id = -1 # for consolidating stream segments
	
	def calculate_reach_order(self):
		if len(self.children_orders) != self.children_number:
			print self.children_orders
			print self.children_number
			raise Exception("count of children orders does not match children number")

		max = 0
		for order in self.children_orders:
			if order > max:
				max = order

		increment_order = False
		max_count = 0
		for order in self.children_orders:
			if order == max:
				max_count += 1

		if max_count > 1:
			return max + 1
		else:
			return max	

unvisited = dict()
assigned = dict()
transmitted = list()
orders = 0

conn = psycopg2.connect(database="streams", password="", host="127.0.0.1", port="5432")
cur = conn.cursor()

cur.execute("update \"" + junctions_table +"\" set node_order = -1");
cur.execute("update \"" + streams_table +"\" set stream_order = -1");
cur.execute("update \"" + streams_table +"\" set base_stream_id = -1");

# assign the first over nodes
update = "update \"" + junctions_table +"\" junctions " \
						"set node_order = 1 where gid in ( " \
						"select node_id from reach_adjacencies group by node_id having count(stream_id) = 1 " \
						") "
cur.execute( update )


# if flow direction is taken into considering, reach_adjacencies would not have the upstream connection, 
# so children_number wouldn't need to subtract 1
# this now takes elevation into account and doesn't subtract 1
sql = " select junctions1.gid, junctions1.node_order, count(ra1.nextval) " \
		"from reach_adjacencies ra1, reach_adjacencies ra2, " \
		+ junctions_table + " junctions1 , " + junctions_table + " junctions2 " \
		"where ra1.stream_id = ra2.stream_id " \
		"and junctions1.gid = ra1.node_id " \
		"and junctions2.gid = ra2.node_id " \
		"and junctions1.elevation >= junctions2.elevation " \
		"and junctions1.gid != junctions2.gid " \
		"group by junctions1.gid, junctions1.node_order "
print sql
cur.execute(sql)


# load the nodes
row = cur.fetchone()
while row != None:
	node = Node()
	node.id = row[0]
	node.order = row[1]
	node.children_number = row[2]
	cur2 = conn.cursor()
	cur2.execute("select nextval('base_stream_id_sequences') ")
	seq = cur2.fetchone()
	node.base_stream_id = seq[0]
	
	if node.order == -1:
		unvisited[node.id] = node
	elif node.order > 0:
		if node.order not in assigned:
			assigned[node.order] = list()
			orders = node.order

		assigned[node.order].append(node)

	row = cur.fetchone()

cur.close()

# Run the stream ordering algorithm 
cur = conn.cursor()
cur2 = conn.cursor()
while True:
	node = None
	# find next node in list that has not been moved to 'assigned'
	list_order = 0
	for i in range(1, orders+1):
		list_order = i
		if len( assigned[i] ) > 0:
			node = assigned[i][0]
			break
	if node == None:
		break

	# remove from assigned
	#print len(assigned[list_order])
	del assigned[list_order][0]

	# get nodes connected to this node that haven't been assigned yet
	# we are assuming for now only one father node (outflow), anabranched could have multiple
	# only look at connections flowing down (higher elevation to lower elevation)
	query = "select ra2.node_id, ra1.stream_id from reach_adjacencies ra1, reach_adjacencies ra2, " \
					+ junctions_table + " junctions1 , " + junctions_table + " junctions2 " \
					"where ra1.stream_id = ra2.stream_id " \
					"and junctions1.gid = ra1.node_id " \
					"and junctions2.gid = ra2.node_id " \
					"and junctions1.elevation >= junctions2.elevation " \
					"and junctions1.gid != junctions2.gid " \
					"and ra1.node_id = %s "  \
					"and junctions2.node_order = -1" % (node.id)
	cur2.execute(query)

	row = cur2.fetchone()
	if row == None:
		# either we've got a disconnected segment, or there's an error in the data
		# note in the logs, then move on
		print "Detected a disconnected segment attached to node id %d " % node.id
		continue

	while row != None:
		if row[0] in unvisited.keys():
			father_node = unvisited[row[0]]
		else:
			print "Key for father node not found, possible elevation misordering: " + str(row[0])
			row = cur2.fetchone()
			continue

		father_node.children_orders.append(node.order)
		#print len(father_node.children_orders) 
		#print father_node.children_number
		if len(father_node.children_orders) == father_node.children_number:
			# we have all the children, calculate the reach order
			#print father_node.id
			#for p in father_node.children_orders:
			#	print "child ", p
			#print "children num", father_node.children_number
			order = father_node.calculate_reach_order()	
			father_node.order = order
			if order not in assigned:
				assigned[order] = list()
				orders = order
			#print "put", father_node.id, " in", father_node.order
			# update in the database
			cur.execute("update \""  + junctions_table + "\" set node_order = %s where gid = %s", [father_node.order, father_node.id])

			if father_node.children_number > 1:
				#print "assigning new base stream id %i" % father_node.base_stream_id 
				print father_node.children_number
				cur2 = conn.cursor()
				cur2.execute("select nextval('base_stream_id_sequences') ")
				seq = cur2.fetchone()
				father_node.base_stream_id = seq[0]
			else:
				father_node.base_stream_id = node.base_stream_id
				#print "transmitting base stream id %i" % father_node.base_stream_id 

			assigned[father_node.order].append(father_node)

			# we can transmit to the outflow reaches here
			# get the ouflows and update them
			cur.execute("update " + streams_table + " set stream_order = %s where gid = %s", [father_node.order, outflow_gid])
			#print "writing %i" % node.base_stream_id
			query = "update " + streams_table + " set base_stream_id = %s where gid = %s" % (father_node.base_stream_id, outflow_gid)
			#print query
			cur.execute(query)
			conn.commit()



			#print "del unvisited %s",  father_node.id
			del unvisited[father_node.id]

		else:
			father_node.base_stream_id = node.base_stream_id
			#print "transmitting base stream id %i" % father_node.base_stream_id 


		transmitted.append(node)
		row = cur2.fetchone()


conn.commit()

# Now everything in the transmitted bucket is both assigned an order and transmitted forward
# Any nodes left in the assigned bucket are root nodes


