import synthdb as s
c = s.connect("https://dev_server", key_file="/Users/psymac0/GitHub/SynthDB/synthdb_node/secure.key")
g = s.graph('price3')
print g.node(42).breadth_first(dist=2, direction="in").pagerank().sort(reverse=True).coerce_to('array').run(c)
