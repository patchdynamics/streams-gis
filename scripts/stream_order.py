#!/usr/bin/python
print 'Here we go!'
import psycopg2

junctions_table = "passumpsic_junctions_fixed"
streams_table = "passumpsichigh"

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

cur.execute("update \"" + junctions_table +"\" set node_order = -1 where node_order != -2");
cur.execute("update \"" + streams_table +"\" set stream_order = -1");
cur.execute("update \"" + streams_table +"\" set base_stream_id = -1");

# assign the first order nodes
update = "update \"" + junctions_table +"\" junctions " \
						"set node_order = 1 where gid in ( " \
						"select node_id from reach_adjacencies group by node_id having count(stream_id) = 1 " \
						") and node_order != -2 "
cur.execute( update )


# if flow direction is taken into considering, reach_adjacencies would not have the upstream connection, 
# so children_number wouldn't need to subtract 1
cur.execute("select gid, node_order, count(reach_adjacencies.nextval) - 1 as children_number from \""  + junctions_table + "\" junctions "
						"join reach_adjacencies on reach_adjacencies.node_id = junctions.gid group by gid, node_order")



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
	query = "select ra2.node_id, ra1.stream_id from reach_adjacencies ra1, reach_adjacencies ra2, \""  + junctions_table + "\" junctions " \
					"where ra1.stream_id = ra2.stream_id and ra1.node_id = %s and junctions.gid = ra2.node_id and junctions.node_order = -1" % (node.id)
	#print query
	cur.execute(query)

	row = cur.fetchone()
	if row == None:
		# either we've got a disconnected segment, or there's an error in the data
		# note in the logs, then move on
		print "Detected a disconnected segment attached to node id %d " % node.id
		continue

		
	father_node = unvisited[row[0]]
	outflow_gid = row[1]

	# we can transmit to the reach here
	cur.execute("update " + streams_table + " set stream_order = %s where gid = %s", [list_order, outflow_gid])
	print "writing %i" % node.base_stream_id
	query = "update " + streams_table + " set base_stream_id = %s where gid = %s" % (node.base_stream_id, outflow_gid)
	print query
	cur.execute(query)
	conn.commit()

	father_node.children_orders.append(node.order)
	if len(father_node.children_orders) == father_node.children_number:
		# we have all the children, calculate the reach order

		if father_node.order == -2:
			# this is the network outflow, don't propagate
			continue

		order = father_node.calculate_reach_order()	
		father_node.order = order
		if order not in assigned:
			assigned[order] = list()
			if orders < order:
				orders = order
		# update in the database
		cur.execute("update \""  + junctions_table + "\" set node_order = %s where gid = %s", [father_node.order, father_node.id])
		print "%i now has order %i" % (father_node.id, father_node.order)

		#if father_node.children_number > 1:
		if father_node.order != node.order: 
			# assign base_stream_id for consolidation when order changes at this node
			print "assigning new base stream id %i" % father_node.base_stream_id 
			print father_node.children_number
			cur2 = conn.cursor()
			cur2.execute("select nextval('base_stream_id_sequences') ")
			seq = cur2.fetchone()
			father_node.base_stream_id = seq[0]
		else:
			father_node.base_stream_id = node.base_stream_id
			print "transmitting base stream id %i" % father_node.base_stream_id 

		assigned[father_node.order].append(father_node)

		#print "del unvisited %s",  father_node.id
		del unvisited[father_node.id]

	else:
		father_node.base_stream_id = node.base_stream_id
		print "transmitting base stream id %i" % father_node.base_stream_id 


	transmitted.append(node)
conn.commit()

# Now everything in the transmitted bucket is both assigned an order and transmitted forward
# Any nodes left in the assigned bucket are root nodes


