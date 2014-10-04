print 'Hello, World'
import psycopg2

class Node():
	def __init__(self):
		self.id = -1
		self.order = -1
		self.children_orders = []
		self.children_number = -1
	
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
orders = 0
transmitted = list()

conn = psycopg2.connect(database="streams", password="", host="127.0.0.1", port="5432")
cur = conn.cursor()

cur.execute("select id, node_order, count(reach_adjacencies.nextval) - 1 as children_number from reach_nodes join reach_adjacencies on reach_adjacencies.node_id = reach_nodes.id group by id, node_order")

row = cur.fetchone()
while row != None:
	node = Node()
	node.id = row[0]
	node.order = row[1]
	node.children_number = row[2]
	
	if node.order == -1:
		unvisited[node.id] = node
	elif node.order > 0:
		if node.order not in assigned:
			assigned[node.order] = list()
			orders = node.order

		assigned[node.order].append(node)

	row = cur.fetchone()

cur.close()

cur = conn.cursor()
while True:
	node = None
	list_order = 0
	for i in range(1, orders+1):
		list_order = i
		if len( assigned[i] ) > 0:
			node = assigned[i][0]
			break
	if node == None:
		break

	# remove from assigned
	print len(assigned[list_order])
	del assigned[list_order][0]

	cur.execute("select ra2.node_id, ra1.gid from reach_adjacencies ra1, reach_adjacencies ra2, reach_nodes rn where ra1.gid = ra2.gid and ra1.node_id = %s and rn.id = ra2.node_id and rn.node_order = -1", [node.id])

	row = cur.fetchone()
	father_node = unvisited[row[0]]
	outflow_gid = row[1]

	# we can transmit to the reach here
	cur.execute("update hubbardnhd set stream_order = %s where gid = %s", [list_order, outflow_gid])

	father_node.children_orders.append(node.order)
	if len(father_node.children_orders) == father_node.children_number:
		# we have all the children, calculate the reach order
		print "calc order"
		print father_node.id
		for p in father_node.children_orders:
			print "child ", p
		print "children num", father_node.children_number
		order = father_node.calculate_reach_order()	
		father_node.order = order
		if order not in assigned:
			assigned[order] = list()
			orders = order
		assigned[father_node.order].append(father_node)
		print "put", father_node.id, " in", father_node.order
		# update in the database
		cur.execute("update reach_nodes set node_order = %s where id = %s", [father_node.order, father_node.id])
		
		print "del unvisited %s",  father_node.id
		del unvisited[father_node.id]

	transmitted.append(node)
conn.commit()

# Now everything in the transmitted bucket is both assigned an order and transmitted forward
# Any nodes left in the assigned bucket are root nodes


