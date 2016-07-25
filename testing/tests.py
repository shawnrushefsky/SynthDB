import synthdb
from random import randint
from datetime import datetime
import preqlerrors
from sys import stdout

dt = datetime.now()
c = synthdb.connect('https://localhost', key_file="/home/ubuntu/synthdb/secure.key")

nn = 100
d = 2
max_iter = 1000


def basic_test(r):
    return type(r).__name__ in ["dict", 'float', 'int', 'list', 'bool']


def err_format(q, resp):
    line = ''.join('-' for i in range(25))
    msg = "QUERY\n" \
          "{}\n" \
          "{}\n" \
          "{}\n" \
          "RESPONSE\n" \
          "{}\n" \
          "{}\n" \
          "{}\n\n".format(line, q, line, line, resp, line)
    return msg

generator_funcs = {
    'price_network': {
        'required': {
            'N': nn
        },
        'optional': {
            'm': 3,
            'c': 0.5,
            'gamma': 0.9
        }
    },
    'triangulation': {
        'required': {
            'N': nn,
            'd': d
        },
        'optional': {
            'type': 'delaunay',
            'periodic': True
        }
    },
    'lattice': {
        'required': {
            'shape': [30, 30]
        },
        'optional': {
            'periodic': True
        }
    },
    'complete_graph': {
        'required': {
            'N': nn
        },
        'optional': {
            'self_loops': True,
            'directed': True
        }
    },
    'circular_graph': {
        'required': {
            'N': nn
        },
        'optional': {
            'k': 3,
            'self_loops': True,
            'directed': True
        }
    },
    'geometric_graph': {
        'required': {
            'N': nn,
            'd': d,
            'radius': 0.2
        },
        'optional': {
            'ranges': [[0.1, 0.3], [0.1, 0.5]]
        }
    }
}

centrality1 = {
    'pagerank': {
        'required': {
            'nprop': 'pagerank'
        },
        'optional': {
            'damping': 0.91,
            'pers': 'n_betweenness',
            'weight': 'l_betweenness',
            'epsilon': 0.000002,
            'max_iter': max_iter,
            'ret_iter': True
        },
    },
    'betweenness': {
        'required': {
            'nprop': 'n_betweenness',
            'lprop': 'l_betweenness'
        },
        'optional': {
            'weight': 'l_betweenness',
            'norm': False
        }
    },
    'closeness': {
        'required': {
            'nprop': 'closeness'
        },
        'optional': {
            'weight': 'l_betweenness',
            'source': 0,
            'norm': False,
            'harmonic': True
        }
    },
    'eigenvector': {
        'required': {
            'nprop': 'eigenvector'
        },
        'optional': {
            'weight': 'l_betweenness',
            'epsilon': 0.000002,
            'max_iter': max_iter
        }
    },
    'katz': {
        'required': {
            'nprop': 'katz',
            'max_iter': max_iter
        },
        'optional': {
            'weight': 'l_betweenness',
            'alpha': 0.02,
            'beta': 'pagerank',
            'epsilon': 0.000002,
            'norm': False
        }
    },
    'hits': {
        'required': {
            'auth_prop': 'hits_auth',
            'hub_prop': 'hits_hub',
            'max_iter': max_iter
        },
        'optional': {
            'weight': 'l_betweenness',
            'epsilon': 0.000002,
            'max_iter': max_iter
        }
    }
}

centrality2 = {
    'central_point_dominance': {
        'required': {
            'betweenness': 'n_betweenness'

        },
        'optional': {}
    },
    'eigentrust': {
        'required': {
            'trust_map': 'l_betweenness',
            'nprop': 'eigentrust'
        },
        'optional': {
            'epsilon': 0.000002,
            'max_iter': max_iter,
            'norm': False,
            'ret_iter': True
        }
    },
    'trust_transitivity': {
        'required': {
            'trust_map': 'l_betweenness',
            'nprop': 'trust_transitivity'
        },
        'optional': {
            'origin': 0,
        }
    }
}

node_cent1 = centrality1.copy()
node_cent1['hits_hub'] = node_cent1['hits'].copy()
node_cent1['hits_authority'] = node_cent1['hits'].copy()
del node_cent1['hits']

layouts = {
    'sfdp': {
        'required': {
            'pos': 'sfdp'
        },
        'optional': {
            'nweight': 'n_betweenness',
            'lweight': 'l_betweenness',
            'C': 0.3,
            'K': 0.15,
            'p': 1.8,
            'theta': 0.7,
            'max_level': 17,
            'gamma': 1.1,
            'mu': 0.1,
            'mu_p': 1.1,
            'init_step': 0.7,
            'cooling_step': .9,
            'adaptive_cooling': False,
            'epsilon': 0.095,
            'max_iter': max_iter,
            'multilevel': True,
            'coarse_method': 'mivs',
            'mivs_thres': 0.8,
            'weighted_coarse': True
        }
    },
    'fruchterman_reingold': {
        'required': {
            'pos': 'fruchterman_reingold'
        },
        'optional': {
            'weight': 'l_betweenness',
            'a': 100.78,
            'r': 1.4,
            'scale': 13.45,
            'circular': True,
            'grid': False,
            't_range': [1.1, 0.031],
            'n_iter': 105
        }
    },
    'arf': {
        'required': {
            'pos': 'arf'
        },
        'optional': {
            'weight': 'l_betweenness',
            'a': 11.78,
            'd': 0.65,
            'dt': .002,
            'epsilon': .00002,
            'max_iter': max_iter,
            'dim': 3
        }
    },
    'radial_tree': {
        'required': {
            'root': 0,
            'pos': 'radial_tree'
        },
        'optional': {
            'weighted': True,
            'node_weight': 'pagerank',
            'r': 1.1
        }
    },
    'random_layout': {
        'required': {
            'pos': 'random_layout'
        },
        'optional': {
            'shape': [30, 30, 30],
            'dim': 3
        }
    }
}

node_topo = ['all_links', 'out_links', 'in_links', 'all_neighbors', 'in_degree', 'out_degree', 'in_neighbors', 'out_neighbors']

link_topo = ['origin', 'terminus']

lweight = "l_betweenness"

topo_queries = {
    'shortest_distance': {
        'required': {
            'dist_map': 'shortest_from_0',
        },
        'optional': {
            'origin': 0,
            'weights': lweight,
            'negative_weights': True,
            'max_dist': 5,
            'directed': False,
            'pred_map': True,
            'dist_map': 'shortest_from_0_options',
        }
    },
    'pseudo_diameter': {
        'required': {},
        'optional': {
            'origin': 0,
            'weights': lweight
        }
    },
    'is_bipartite': {
        'required': {},
        'optional': {
            'partition': True
        }
    },
    'is_planar': {
        'required': {},
        'optional': {
            'embedding': True,
            'kuratowski': True,
        }
    },
    'max_cardinality_matching': {
        'required': {
            'match': 'max_cardinal'
        },
        'optional': {
            'heuristic': True,
            'weight': lweight,
            'minimize': False,
            'match': 'max_cardinal_options'
        }
    },
    'max_independent_node_set': {
        'required': {
            'mivs': 'mivs'
        },
        'optional': {
            'high_deg': True,
            'mivs': 'mivs_option'
        }
    },
    'link_reciprocity': {
        'required': {},
        'optional': {}
    },
    'tsp_tour': {
        'required': {
            'origin': 0
        },
        'optional': {}
    },
    'sequential_node_coloring': {
        'required': {
            'color': 'colors'
        },
        'optional': {
            'color': 'colors_options'
        }
    },
    'min_spanning_tree': {
        'required': {
            'tree_map': 'min_tree'
        },
        'optional': {
            'root': 0,
            'tree_map': 'min_tree_options'
        }
    },
    'random_spanning_tree': {
        'required': {
            'tree_map': 'random_tree'
        },
        'optional': {
            'weights': lweight,
            'root': 0,
            'tree_map': 'random_tree_options'
        }
    },
    'dominator_tree': {
        'required': {
            'root': 0,
            'dom_map': 'dom_tree'
        },
        'optional': {
            'dom_map': 'dom_tree_options'
        }
    },
    'topological_sort': {
        'required': {},
        'optional': {}
    },
    'kcore_decomposition': {
        'required': {
            'nprop': 'kcore'
        },
        'optional': {
            'deg': 'total',
            'nprop': 'kcore_options'
        }
    }
}

total_queries = 0
passes = 0
fails = []
errors = []


def try_it(qu):
    stdout.write("\r{} ---> ".format(qu))
    stdout.flush()
    passed = 0
    req = None
    try:
        req = qu.run(c)
        if basic_test(req):
            passed = 1
            stdout.write("PASS\n")
        else:
            fails.append(err_format(q, req))
            stdout.write("FAIL\n")
            print err_format(q, req)
            exit()
    except (preqlerrors.TopologyError, preqlerrors.ValueTypeError, preqlerrors.NonexistenceError) as e:
        errors.append(err_format(q, str(e.msg)))
        stdout.write("ERROR\n")
    stdout.flush()
    return passed, 1, req


for k in generator_funcs:
    g_name = k[:4]+"_default"
    params = generator_funcs[k]['required']
    q = "synthdb.create_graph('{}').{}({}).run(c)".format(g_name, k, preqlerrors.param_stringer(params))
    qu = getattr(synthdb.create_graph(g_name), k)(**params)
    pa, tq, req = try_it(qu)
    passes += pa
    total_queries += tq

for k in generator_funcs:
    g_name = k[:4] + "_options"
    params = generator_funcs[k]['required'].copy()
    params.update(generator_funcs[k]['optional'])
    q = "synthdb.create_graph('{}').{}({}).run(c)".format(g_name, k, preqlerrors.param_stringer(params))
    qu = getattr(synthdb.create_graph(g_name), k)(**params)
    pa, tq, req = try_it(qu)
    passes += pa
    total_queries += tq

for g in synthdb.list_graphs().run(c):
    for k in centrality1:
        params = centrality1[k]['required'].copy()
        q = "synthdb.graph('{}').{}({}).run(c)".format(g, k, preqlerrors.param_stringer(params))
        qu = getattr(synthdb.graph(g), k)(**params)
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

    for k in centrality2:
        params = centrality2[k]['required'].copy()
        q = "synthdb.graph('{}').{}({}).run(c)".format(g, k, preqlerrors.param_stringer(params))
        qu = getattr(synthdb.graph(g), k)(**params)
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

    for k in centrality1:
        params = centrality1[k]['required'].copy()
        params.update(centrality1[k]['optional'])
        if 'nprop' in params:
            params['nprop'] += "_o"
        if 'lprop' in params:
            params['lprop'] += "_o"
        if 'auth_prop' in params:
            params['auth_prop'] += "_o"
            params['hub_prop'] += "_o"
        q = "synthdb.graph('{}').{}({}).run(c)".format(g, k, preqlerrors.param_stringer(params))
        qu = getattr(synthdb.graph(g), k)(**params)
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

    for k in centrality2:
        params = centrality2[k]['required'].copy()
        params.update(centrality2[k]['optional'])
        if 'nprop' in params:
            params['nprop'] += "_o"
        if 'lprop' in params:
            params['lprop'] += "_o"
        if 'auth_prop' in params:
            params['auth_prop'] += "_o"
            params['hub_prop'] += "_o"
        q = "synthdb.graph('{}').{}({}).run(c)".format(g, k, preqlerrors.param_stringer(params))
        qu = getattr(synthdb.graph(g), k)(**params)
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

    for k in layouts:
        params = layouts[k]['required'].copy()
        q = "synthdb.graph('{}').{}({}).run(c)".format(g, k, preqlerrors.param_stringer(params))
        qu = getattr(synthdb.graph(g), k)(**params)
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

    for k in layouts:
        params = layouts[k]['required'].copy()
        params.update(layouts[k]['optional'])
        if 'pos' in params:
            params['pos'] += '_o'
        q = "synthdb.graph('{}').{}({}).run(c)".format(g, k, preqlerrors.param_stringer(params))
        qu = getattr(synthdb.graph(g), k)(**params)
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

    q = "synthdb.graph('{}').nodes().count().run(c)".format(g)
    qu = synthdb.graph(g).nodes().count()
    pa, tq, num_nodes = try_it(qu)
    passes += pa
    total_queries += tq

    q = "synthdb.graph('{}').links().count().run(c)".format(g)
    qu = synthdb.graph(g).links().count()
    pa, tq, num_links = try_it(qu)
    passes += pa
    total_queries += tq

    print "{} nodes, {} links".format(num_nodes, num_links)
    rand_node_ids = [randint(0, num_nodes-1) for i in range(20)]
    for n in rand_node_ids:
        q = "synthdb.graph('{}').node({}).run(c)".format(g, n)
        qu = synthdb.graph(g).node(n)
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

        q = "synthdb.graph('{}').property_map('pagerank').get({}).run(c)".format(g, n)
        qu = synthdb.graph(g).property_map('pagerank').get(n)
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

    q = "synthdb.graph('{}').nodes({}).run(c)".format(g, rand_node_ids)
    qu = synthdb.graph(g).nodes(rand_node_ids).coerce_to('array')
    pa, tq, rand_nodes = try_it(qu)
    passes += pa
    total_queries += tq

    rand_node_uids = [n['uid'] for n in rand_nodes]
    q = "synthdb.graph('{}').nodes({}, index='uid').run(c)".format(g, rand_node_uids)
    qu = synthdb.graph(g).nodes(rand_node_uids, index='uid').coerce_to('array')
    pa, tq, rand_nodes_by_uid = try_it(qu)
    passes += pa
    total_queries += tq

    q = "synthdb.graph('{}').property_map('pagerank').get_all({}).run(c)".format(g, rand_node_ids)
    qu = synthdb.graph(g).property_map('pagerank').get_all(rand_node_ids).coerce_to('array')
    pa, tq, req = try_it(qu)
    passes += pa
    total_queries += tq

    comp_set1 = {"{}_{}".format(n['id'], n['uid']) for n in rand_nodes}
    comp_set2 = {"{}_{}".format(n['id'], n['uid']) for n in rand_nodes_by_uid}
    samesies = comp_set1 == comp_set2
    print "Node ID <-> UID Matches: {}".format(samesies)
    if not samesies:
        exit()

    links = []
    for k in node_topo:
        for n in rand_node_ids:
            q = "synthdb.graph('{}').node({}).{}().run(c)".format(g, n, k)
            qu = getattr(synthdb.graph(g).node(n), k)()
            pa, tq, r = try_it(qu)
            passes += pa
            total_queries += tq
            if k == "all_links":
                links += r

    link_ids = list({l['id'] for l in links})
    link_uids = list({l['uid'] for l in links})
    for l in link_ids:
        q = "synthdb.graph('{}').link('{}').run(c)".format(g, l)
        qu = synthdb.graph(g).link(l)
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

        q = "synthdb.graph('{}').property_map('{}').get('{}')".format(g, lweight, l)
        qu = synthdb.graph(g).property_map(lweight).get(l)
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

    q = "synthdb.graph('{}').links({}).run(c)".format(g, link_ids)
    qu = synthdb.graph(g).links(link_ids).coerce_to('array')
    pa, tq, links_by_id = try_it(qu)
    passes += pa
    total_queries += tq

    q = "synthdb.graph('{}').links({}, index='uid').run(c)".format(g, link_uids)
    qu = synthdb.graph(g).links(link_uids, index='uid').coerce_to('array')
    pa, tq, links_by_uid = try_it(qu)
    passes += pa
    total_queries += tq

    q = "synthdb.graph('{}').property_map('{}').get_all({}).run(c)".format(g, lweight, link_ids)
    qu = synthdb.graph(g).property_map(lweight).get_all(link_ids).coerce_to('array')
    pa, tq, req = try_it(qu)
    passes += pa
    total_queries += tq

    comp_set1 = {"{}_{}".format(n['id'], n['uid']) for n in links_by_id}
    comp_set2 = {"{}_{}".format(n['id'], n['uid']) for n in links_by_uid}
    samesies = comp_set1 == comp_set2
    print "Link ID <-> UID Matches: {}".format(samesies)
    if not samesies:
        exit()

    for k in link_topo:
        for l in link_ids:
            q = "synthdb.graph('{}').link('{}').{}()".format(g, l, k)
            qu = getattr(synthdb.graph(g).link(l), k)()
            pa, tq, req = try_it(qu)
            passes += pa
            total_queries += tq

    params = {
        'origin': rand_node_ids[0],
        'terminus': rand_node_ids[1]
    }
    q = "synthdb.graph('{}').{}({}).run(c)".format(g, "shortest_path", preqlerrors.param_stringer(params))
    qu = synthdb.graph(g).shortest_path(**params)
    pa, tq, req = try_it(qu)
    passes += pa
    total_queries += tq

    params['weights'] = lweight
    params['negative_weights'] = True
    params['pred_map'] = True
    q = "synthdb.graph('{}').{}({}).run(c)".format(g, "shortest_path", preqlerrors.param_stringer(params))
    qu = synthdb.graph(g).shortest_path(**params)
    pa, tq, req = try_it(qu)
    passes += pa
    total_queries += tq

    q = "synthdb.graph('{}').{}().run(c)".format(g, "is_DAG")
    qu = synthdb.graph(g).is_DAG()
    pa, tq, is_dag = try_it(qu)
    passes += pa
    total_queries += tq

    for k in topo_queries:
        if k in ["topological_sort"] and not is_dag:
            print "Skipping {}.{} because it is not a DAG".format(g, k)
            continue
        params = topo_queries[k]['required'].copy()
        q = "synthdb.graph('{}').{}({}).run(c)".format(g, k, preqlerrors.param_stringer(params))
        qu = getattr(synthdb.graph(g), k)(**params)
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

    q = "synthdb.graph('{}').{}().run(c)".format(g, "is_DAG")
    qu = synthdb.graph(g).is_DAG()
    pa, tq, is_dag = try_it(qu)
    passes += pa
    total_queries += tq

    for k in topo_queries:
        if k in ["topological_sort"] and not is_dag:
            print "Skipping {}.{} because it is not a DAG".format(g, k)
            continue
        params = topo_queries[k]['required'].copy()
        params.update(topo_queries[k]['optional'])
        q = "synthdb.graph('{}').{}({}).run(c)".format(g, k, preqlerrors.param_stringer(params))
        qu = getattr(synthdb.graph(g), k)(**params)
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

    upd = "{'an': {'arbitrarily': {'nested': 'value'}}}"
    for n in synthdb.graph(g).nodes()['id'].run(c):
        q = "synthdb.graph('{}').node({}).update({}).run(c)".format(g, n, upd)
        qu = synthdb.graph(g).node(n).update({'an': {'arbitrarily': {'nested': 'value'}}})
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

    for l in synthdb.graph(g).links()['id'].run(c):
        q = "synthdb.graph('{}').link('{}').update({}).run(c)".format(g, l, upd)
        qu = synthdb.graph(g).link(l).update({'an': {'arbitrarily': {'nested': 'value'}}})
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

    for pm in synthdb.graph(g).property_maps().run(c):
        q = "synthdb.graph('{}').property_map('{}').commit('{}').run(c)".format(g, pm['id'], pm['id'])
        qu = synthdb.graph(g).property_map(pm['id']).commit(pm['id'])
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

        q = "synthdb.graph('{}').property_map('{}').sort(reverse=True).limit(10).coerce_to('array').run(c)".format(g, pm['id'])
        qu = synthdb.graph(g).property_map(pm['id']).sort(reverse=True).limit(10).coerce_to('array')
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

    q = "synthdb.graph('{}').nodes().map(lambda n: n['id']).reduce(lambda x, y: x+y).run(c)".format(g)
    qu = synthdb.graph(g).nodes().map(lambda n: n['id']).reduce(lambda x, y: x+y)
    pa, tq, req = try_it(qu)
    passes += pa
    total_queries += tq

    for n_id in rand_node_ids:
        directions = ['in', 'out']
        for i in range(1, 5):
            params = {
                'dist': i,
                'direction': [directions[randint(0, 1)] for _ in range(i)]
            }
            q = "synthdb.graph('{}').node('{}').breadth_first({}).run(c)".format(g, n_id, preqlerrors.param_stringer(params))
            qu = synthdb.graph(g).node(n_id).breadth_first(**params)
            pa, tq, req = try_it(qu)
            passes += pa
            total_queries += tq

            for k in node_cent1:
                q_params = node_cent1[k]['required'].copy()
                formats = [g, n_id, preqlerrors.param_stringer(params), k, preqlerrors.param_stringer(q_params)]
                q = "synthdb.graph('{}').node('{}').breadth_first({}).{}({}).run(c)".format(*formats)
                qu = getattr(synthdb.graph(g).node(n_id).breadth_first(**params), k)(**q_params)
                pa, tq, req = try_it(qu)
                passes += pa
                total_queries += tq

                formats = [g, n_id, preqlerrors.param_stringer(params), k, preqlerrors.param_stringer(q_params)]
                q = "synthdb.graph('{}').node('{}').breadth_first({}).{}({}).sort(reverse=True).limit(10).coerce_to('array').run(c)".format(*formats)
                qu = getattr(synthdb.graph(g).node(n_id).breadth_first(**params), k)(**q_params).sort(reverse=True).limit(10).coerce_to('array')
                pa, tq, req = try_it(qu)
                passes += pa
                total_queries += tq

            for k in layouts:
                q_params = layouts[k]['required'].copy()
                formats = [g, n_id, preqlerrors.param_stringer(params), k, preqlerrors.param_stringer(q_params)]
                q = "synthdb.graph('{}').node('{}').breadth_first({}).{}({}).run(c)".format(*formats)
                qu = getattr(synthdb.graph(g).node(n_id).breadth_first(**params), k)(**q_params)
                pa, tq, req = try_it(qu)
                passes += pa
                total_queries += tq

    q = "synthdb.graph('{}').nodes().filter(lambda n: n['pagerank'] > 0.001).coerce_to('array').run(c)".format(g)
    qu = synthdb.graph(g).nodes().filter(lambda n: n['pagerank'] > 0.001).coerce_to('array')
    pa, tq, req = try_it(qu)
    passes += pa
    total_queries += tq

    q = "synthdb.graph('{}').links().filter(lambda l: l['l_betweenness'] > 0.001).coerce_to('array').run(c)".format(g)
    qu = synthdb.graph(g).links().filter(lambda l: l['l_betweenness'] > 0.001).coerce_to('array')
    pa, tq, req = try_it(qu)
    passes += pa
    total_queries += tq

    q = "synthdb.graph('{}').nodes().map(lambda n: n['pagerank']).coerce_to('array').run(c)".format(g)
    qu = synthdb.graph(g).nodes().map(lambda n: n['pagerank']).coerce_to('array')
    pa, tq, req = try_it(qu)
    passes += pa
    total_queries += tq

    q = "synthdb.graph('{}').links().map(lambda l: l['l_betweenness']).coerce_to('array').run(c)".format(g)
    qu = synthdb.graph(g).links().map(lambda l: l['l_betweenness']).coerce_to('array')
    pa, tq, req = try_it(qu)
    passes += pa
    total_queries += tq

    for l in link_uids[:50]:
        q = "synthdb.graph('{}').link('{}').delete().run(c)".format(g, l)
        qu = synthdb.graph(g).link(l).delete()
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

    for n_id in rand_node_uids:
        q = "synthdb.graph('{}').node('{}').delete().run(c)".format(g, n_id)
        qu = synthdb.graph(g).node(n_id).delete()
        pa, tq, req = try_it(qu)
        passes += pa
        total_queries += tq

    q = "synthdb.graph('{}').links().delete().run(c)".format(g)
    qu = synthdb.graph(g).links().delete()
    pa, tq, req = try_it(qu)
    passes += pa
    total_queries += tq

    q = "synthdb.graph('{}').nodes().delete().run(c)".format(g)
    qu = synthdb.graph(g).nodes().delete()
    pa, tq, req = try_it(qu)
    passes += pa
    total_queries += tq


for f in fails:
    print "FAIL: {}".format(f)

for e in errors:
    print "ERROR: {}".format(e)

print "{} queries tested. Total running time: {}".format(total_queries, datetime.now() - dt)
print "{} Successes, {} Errors, {} Fails:".format(passes, len(errors), len(fails))