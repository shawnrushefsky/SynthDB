# SynthDB Python Driver
# Made available under the MIT License (MIT)
# Copyright (c) 2016 Psymphonic LLC
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
# WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
# OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT
# OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# coding = utf-8
import requests
import simplejson as json
from urllib2 import quote
import re
from sys import stdout
from cloud.serialization.cloudpickle import dumps as pickledumps
from posixpath import join as urljoin
import rethinkdb
import preqlerrors
from datetime import datetime
from os.path import join
from copy import deepcopy as copy
import time
import csv

rep = {'\n': '%0A', '\t': '%09', '\r': '%0D', '\b': '%08', '\\': '%5C', '\"': '%22', '\x00': '%00',
       '\x01': '%01', '\x02': '%02', '\x03': '%03', '\x04': '%04', '\x05': '%05', '\x06': '%06', '\x07': '%07',
       '\x08': '%08', '\x09': '%09', '\x0a': '%0A', '\x0b': '%0B', '\x0c': '%0C', '\x0d': '%0D', '\x0e': '%0E',
       '\x0f': '%0F', '\x10': '%10', '\x11': '%11', '\x12': '%12', '\x13': '%13', '\x14': '%14', '\x15': '%15',
       '\x16': '%16', '\x17': '%17', '\x18': '%18', '\x19': '%19', '\x1a': '%1A', '\x1b': '%1B', '\x1c': '%1C',
       '\x1d': '%1D', '\x1e': '%1E', '\x1f': '%1F', '\xff': '<?>', '\x91': '%91', '\x82': '%82', '\xa0': '%a0',
       '\xe9': '%e9', '\xc3': '\%c3'}
rep = dict((re.escape(k), v) for k, v in rep.iteritems())
pattern = re.compile("|".join(rep.keys()), re.I)

error_format = preqlerrors.error_format

requests.packages.urllib3.disable_warnings()

errors = preqlerrors


class NonExistenceError(Exception):
    def __init__(self, q, message, wrong_val):
        msg = error_format("NonExistenceError", q, wrong_val, message)
        self.message = msg
        print msg


class PreqlDriverError(Exception):
    def __init__(self, q, message, wrong_val):
        msg = error_format("PreqlDriverError", q, wrong_val, message)
        self.message = msg
        print msg


class GraphLoader(object):
    def __init__(self, name=None):
        self.name = name

    def node_types(self):
        raise NotImplementedError('Subclasses of GraphLoader require a node_types() method.')

    def link_types(self):
        raise NotImplementedError('Subclasses of GraphLoader require a link_types() method.')

    def nodes(self):
        raise NotImplementedError('Subclasses of GraphLoader require a nodes() method.')

    def links(self):
        raise NotImplementedError('Subclasses of GraphLoader require a links() method.')


class MovieGraph(GraphLoader):
    def __init__(self, directory):
        self.root_dir = directory
        GraphLoader.__init__(self, 'Movies')

    def node_types(self):
        return [{'id': v} for v in ['movie', 'person', 'genre', 'country', 'language']]

    def link_types(self):
        return [{'id': v} for v in ['acted', 'directed', 'genre', 'wrote', 'made_in', 'language']]

    def nodes(self):
        with open(join(self.root_dir, 'nodes.txt')) as f:
            for line in f:
                yield json.loads(line)

    def links(self):
        with open(join(self.root_dir, 'links.txt')) as f:
            for line in f:
                yield json.loads(line)


def rate_limited(max_per_second):

    min_interval = 1.0 / float(max_per_second)

    def decorate(func):
        last_time_called = [0.0]

        def rate_limited_function(*args, **kwargs):
            elapsed = time.time() - last_time_called[0]
            left_to_wait = min_interval - elapsed
            if left_to_wait > 0:
                time.sleep(left_to_wait)
            ret = func(*args, **kwargs)
            last_time_called[0] = time.time()
            return ret
        return rate_limited_function
    return decorate


def csv_unicode(file_path, **csv_params):
    def field_format(value):
        if type(value).__name__ == "str":
            try:
                return unicode(value, encoding='utf-8', errors='xmlcharrefreplace')
            except TypeError:
                return unicode(value, encoding='utf-8', errors='ignore')
        else:
            return value
    with open(file_path) as f:
        for line in csv.reader(f, **csv_params):
            yield [field_format(v) for v in line]


def csv_json(file_path, column_names, **csv_params):
    for line in csv_unicode(file_path, **csv_params):
        yield {column_names[i]: line[i] for i in range(len(line))}


def connect(host="http://127.0.0.1:7796/", key_file=None, verify=False):
    if "http://" not in host and 'https://' not in host:
        host = "http://"+host
    if host[-1] != '/':
        host += '/'
    error_params = ["SynthDB.connect('{}')".format(host), "No instance of SynthDB found at {}".format(host), host]
    if key_file is not None:
        with open(key_file) as f:
            key = f.read()
    else:
        key = None
    try:
        r = requests.get(urljoin(host, 'preql/q'), verify=verify, headers={'Api-Key': key, 'q': 'ping'})
        if r.text == "Hi there!":
            return Connection(host, key, verify)
        else:
            raise PreqlDriverError(*error_params)
    except requests.ConnectionError:
        raise PreqlDriverError(*error_params)


def graph(graph_id):
    return PreqlQuery(graph_id)


branch = rethinkdb.branch
field = rethinkdb.row
expr = rethinkdb.expr
js = rethinkdb.js
literal = rethinkdb.literal


class Runnable(object):
    def __init__(self):
        self.params = {}
        self.q = None
        self.stream = False
        self.body = {}
        self.g = None

    def run(self, c, verbose=False, test=False):
        return c.go(self, verbose, test)


def merge_objects(*dict_args):
    '''
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    '''
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


class create_graph(Runnable):
    def __init__(self, graph_id):
        Runnable.__init__(self)
        self.g = graph_id
        self.q = 'create_graph'
        self.query_string = "create_graph('{}')".format(graph_id)

    def __str__(self):
        return self.query_string


class list_graphs(Runnable):
    def __init__(self):
        Runnable.__init__(self)
        self.q = "list_graphs"


class drop_graph(Runnable):
    def __init__(self, graph_id):
        Runnable.__init__(self)
        self.g = graph_id
        self.q = 'drop_graph'


class Connection(object):
    def __init__(self, host="http://127.0.0.1:7796/", key=None, verify=False):
        self.host = host
        self.api = urljoin(host, 'preql/q')
        self.error_params = ["SynthDB.connect('{}')".format(self.host), "No instance of SynthDB found at {}".format(self.host), self.host]
        self.key = key
        self.verify = verify
        self.delim = '\t'

    @staticmethod
    def __str_quote(string):
        if type(string).__name__ == "str":
            try:
                string = unicode(string, encoding='utf-8', errors='xmlcharrefreplace')
            except TypeError:
                string = unicode(string, encoding='utf-8', errors='ignore')
        elif type(string).__name__ == "unicode":
            try:
                string = string.encode(encoding='utf-8', errors='xmlcharrefreplace')
            except UnicodeEncodeError:
                string = string.encode(encoding='utf-8', errors='ignore')
        string = pattern.sub(lambda m: rep[re.escape(m.group(0))], string)
        try:
            return quote(string, safe='')
        except KeyError:
            print "QUOTE FAIL"
            print type(string).__name__
            print string
            exit()

    def __list_quote(self, l):
        quoted = []
        for v in l:
            if type(v).__name__ == "dict":
                quoted.append(self.__dict_quote(v))
            elif type(v).__name__ == "list":
                quoted.append(self.__list_quote(v))
            elif type(v).__name__ in ["str", "unicode"]:
                quoted.append(self.__str_quote(v))
            else:
                quoted.append(v)
        return quoted

    def __dict_quote(self, d):
        quoted = {}
        for k in d:
            key = quote(k)
            if type(d[k]).__name__ == "dict":
                val = self.__dict_quote(d[k])
            elif type(d[k]).__name__ == "list":
                val = self.__list_quote(d[k])
            elif type(d[k]).__name__ in ["str", "unicode"]:
                val = self.__str_quote(d[k])
            else:
                val = d[k]
            quoted[key] = val
        return quoted

    def __stream_json(self, iterable, verbose=False):
        if verbose:
            for i, obj in enumerate(iterable):
                stdout.write("\r{} transmitted".format(i+1))
                stdout.flush()
                yield json.dumps(obj)+self.delim
            stdout.write('\n')
            stdout.flush()
        else:
            for obj in iterable:
                yield json.dumps(obj)+self.delim

    @staticmethod
    def __handle_response(r):
        try:
            d = r.json()
            if type(d).__name__ == 'dict' and 'error' in d:
                raise preqlerrors.error_classes[d['error']['type']](d['error']['msg'])
        except json.JSONDecodeError:
            d = r.text
        return d

    @staticmethod
    def __validate_stream(r):
        stream = r.iter_lines(delimiter='\t')
        try:
            d = json.loads(next(stream))
        except StopIteration:
            return None, None
        if type(d).__name__ == 'dict' and 'error' in d:
            raise preqlerrors.error_classes[d['error']['type']](d['error']['msg'])
        else:
            return d, stream

    @staticmethod
    def __stream_response(first, stream):
        yield first
        for d in stream:
            if d:
                yield json.loads(d)

    def __get_catch(self, url, stream=False):
        try:
            return requests.get(url, stream=stream, verify=self.verify, headers=self.headers)
        except requests.ConnectionError:
            raise PreqlDriverError(*self.error_params)

    def __put_catch(self, url):
        try:
            return requests.put(url)
        except requests.ConnectionError:
            raise PreqlDriverError(*self.error_params)

    def __prepped_put_catch(self, url, data, headers, stream=False):
        sess = requests.Session()
        req = requests.Request('PUT', url, data=data, headers=headers)
        prepped = req.prepare()
        try:
            return sess.send(prepped, stream=stream, verify=self.verify)
        except requests.ConnectionError:
            raise PreqlDriverError(*self.error_params)

    def __delete_catch(self, url):
        try:
            return requests.delete(url, verify=self.verify, headers=self.headers)
        except requests.ConnectionError:
            raise PreqlDriverError(*self.error_params)

    def __post_catch(self, url, data, headers):
        try:
            return requests.post(url, data=data, verify=self.verify, headers=headers)
        except requests.ConnectionError, msg:
            raise PreqlDriverError(*self.error_params)

    def go(self, query_obj, verbose=False, test=False):
        q = query_obj.q
        headers = {
            'Api-Key': self.key,
            'g': query_obj.g,
            'q': q
        }
        r = None
        if q == "insert":
            headers['params'] = json.dumps(query_obj.params)
            r = self.__post_catch(self.api, data=self.__stream_json(query_obj.body, verbose), headers=headers)
        elif q in ["create_graph", "drop_graph", "list_graphs", "graph_stats"]:
            r = self.__post_catch(self.api, headers=headers, data=None)
        elif q in ["pluck", "stream", "update", "topology", "generate", "commit", "graph_filter", "delete",
                   "create_index", "walk", "fields"]:
            headers['params'] = json.dumps(query_obj.params)
            r = self.__prepped_put_catch(
                self.api, data=pickledumps(query_obj.body), headers=headers, stream=query_obj.stream)
        if r:
            if query_obj.stream:
                f, gen = self.__validate_stream(r)
                if f:
                    return self.__stream_response(f, gen)
                else:
                    return []
            else:
                return self.__handle_response(r)


class PreqlQuery(Runnable):
    def __init__(self, g_id):
        Runnable.__init__(self)
        self.g = g_id
        self.query_string = "graph('{}')".format(g_id)
        self.params['type'] = "graph"
        self.q = "graph_stats"
        self.i = self.walk_in
        self.o = self.walk_out

    def __str__(self):
        return self.query_string

    def __getitem__(self, item):
        nq = copy(self)
        if 'nested' not in nq.body:
            nq.body['nested'] = []
        nq.body['nested'].append(item)
        nq.query_string += "['{}']".format(item)
        return nq

    def filter(self, predicate):
        nq = copy(self)
        if nq.q == "graph_stats":
            nq.q = 'graph_filter'
            nq.stream = False
        nq.body['filter'] = predicate
        nq.query_string += ".filter({})".format(predicate)
        return nq

    def map(self, mapper):
        nq = copy(self)
        if nq.q != "walk":
            nq.body['map'] = mapper
            nq.query_string += ".map({})".format(mapper)
        else:
            if 'nodes' in mapper:
                nq.body['walk_rules']['nmap'] = mapper['nodes']
            if 'links' in mapper:
                nq.body['walk_rules']['lmap'] = mapper['links']
            nq.query_string += ".map({})".format(preqlerrors.param_stringer(mapper))
        return nq

    def reduce(self, reducer):
        nq = copy(self)
        if nq.q != "walk":
            nq.body['reduce'] = reducer
            nq.stream = False
        else:
            nq.body['walk_rules']['reduce'] = reducer
        nq.query_string += ".reduce({})".format(reducer)
        return nq

    def coerce_to(self, data_type, **kwargs):
        nq = copy(self)
        nq.body['coerce_to'] = data_type
        nq.stream = data_type == "stream"
        if data_type == "property_map":
            if 'name' in kwargs:
                nq.body['pmap_name'] = kwargs['name']
            if 'type' in kwargs:
                nq.body['pmap_type'] = kwargs['type']
        ps = preqlerrors.param_stringer(kwargs)
        if ps == "...":
            nq.query_string += ".coerce_to('{}')".format(data_type)
        else:
            nq.query_string += ".coerce_to('{}', {})".format(data_type, ps)
        return nq

    def get(self, obj_id):
        nq = copy(self)
        nq.q = 'pluck'
        nq.body['select'] = obj_id
        nq.stream = False
        nq.query_string += ".get({})".format(preqlerrors.quoted_val(obj_id))
        return nq

    def get_all(self, *args, **kwargs):
        nq = copy(self)
        if len(args) > 1:
            nq.body['get_all'] = list(set(args))
        elif len(args) == 1:
            if type(args[0]).__name__ in ["generator", "list", "tuple"]:
                nq.body['get_all'] = list(set(args[0]))
            else:
                nq.body['get_all'] = [args[0]]
        if 'index' in kwargs:
            nq.body['index'] = kwargs['index']
        if 'uids' in kwargs and kwargs['uids']:
            nq.body['index'] = 'uids'
            nq.body['uids'] = True
        nq.stream = True
        if len(kwargs) > 0:
            nq.query_string += ".get_all({}, {})".format(nq.body['get_all'], preqlerrors.param_stringer(kwargs))
        else:
            nq.query_string += ".get_all({})".format(nq.body['get_all'])
        return nq

    def update(self, upd):
        nq = copy(self)
        nq.q = "update"
        nq.body['update'] = upd
        nq.query_string += ".update({})".format(upd)
        nq.stream = False
        return nq

    def delete(self):
        nq = copy(self)
        nq.q = "delete"
        nq.stream = False
        nq.query_string += ".delete()"
        return nq

    def sort(self, **kwargs):
        nq = copy(self)
        if nq.q != "walk":
            nq.body['sort'] = kwargs
        else:
            nq.body['walk_rules']['sort'] = kwargs
        if not ('coerce_to' in nq.body and nq.body['coerce_to'] == "array"):
            nq.stream = True
        nq.query_string += ".sort({})".format(preqlerrors.param_stringer(kwargs))
        return nq

    def limit(self, number):
        nq = copy(self)
        nq.body['limit'] = number
        if number <= 1:
            nq.stream = False
        nq.query_string += ".limit({})".format(number)
        return nq

    def commit(self, fields):
        nq = copy(self)
        nq.q = "commit"
        nq.stream = False
        nq.body['commit_target'] = fields
        nq.query_string += ".commit({})".format(fields)
        return nq

    def count(self):
        nq = copy(self)
        nq.body['count'] = True
        nq.query_string += ".count()"
        nq.stream = False
        return nq

    def walk_out(self, node=None, link=None):
        nq = copy(self)
        nq.q = "walk"
        if 'walk_rules' not in nq.body:
            nq.body['alg'] = 'breadth_first'
            nq.body['walk_rules'] = {
                'direction': ['out'],
                'dist': 1,
                'filters': []
            }
        else:
            nq.body['walk_rules']['direction'].append('out')
            nq.body['walk_rules']['dist'] += 1
        filt = {}
        if node is not None:
            filt['node'] = node
        if link is not None:
            filt['link'] = link
        nq.body['walk_rules']['filters'].append(filt)
        ps = preqlerrors.param_stringer(filt)
        if ps == "...":
            ps = ''
        nq.query_string += ".walk_out({})".format(ps)
        return nq

    def walk_in(self, node=None, link=None):
        nq = copy(self)
        nq.q = "walk"
        if 'walk_rules' not in nq.body:
            nq.body['alg'] = 'breadth_first'
            nq.body['walk_rules'] = {
                'direction': ['in'],
                'dist': 1,
                'filters': []
            }
        else:
            nq.body['walk_rules']['direction'].append('in')
            nq.body['walk_rules']['dist'] += 1
        filt = {}
        if node is not None:
            filt['node'] = node
        if link is not None:
            filt['link'] = link
        nq.body['walk_rules']['filters'].append(filt)
        ps = preqlerrors.param_stringer(filt)
        if ps == "...":
            ps = ''
        nq.query_string += ".walk_in({})".format(ps)
        return nq

    def all_fields(self):
        nq = copy(self)
        nq.q = "fields"
        nq.query_string += ".all_fields()"
        return nq

    def create_index(self, index_name, definition=None):
        nq = copy(self)
        nq.q = "create_index"
        nq.body['index_name'] = index_name
        nq.body['definition'] = definition
        nq.stream = False
        nq.query_string += ".create_index('{}', definition={})".format(index_name, definition)
        return nq

table_types = ['nodes', 'links', 'node_types', 'link_types']
stream_types = ['property_map', 'array', 'property_maps', 'arrays'] + table_types
doc_types = [s[:-1] for s in table_types]
topo_queries = ['pagerank', 'betweenness', 'closeness', 'eigenvector', 'katz', 'hits', 'hits_hub', 'hits_authority',
                'central_point_dominance', 'eigentrust', 'trust_transitivity', 'sfdp', 'fruchterman_reingold',
                'arf', 'radial_tree', 'random_layout', 'shortest_distance', 'shortest_path', 'pseudo_diameter',
                'is_bipartite', 'is_planar', 'is_DAG', 'max_cardinality_matching', 'max_independent_node_set',
                'link_reciprocity', 'sequential_node_coloring', 'similarity', 'isomorphism',
                'subgraph_isomorphism', 'min_spanning_tree', 'dominator_tree',
                'topological_sort', 'kcore_decomposition', 'tsp_tour', 'random_spanning_tree', 'all_links',
                'out_links', 'in_links', 'all_neighbors', 'in_degree', 'out_degree', 'in_neighbors', 'out_neighbors',
                'origin', 'terminus']
generator_funcs = ['price_network', 'random_graph', 'triangulation', 'lattice', 'complete_graph', 'circular_graph',
                   'geometric_graph']
walkers = ['breadth_first', 'depth_first']


def make_insert_func(cls, iq):
    def f(self, *args, **kwargs):
        nq = copy(self)
        nq.q = 'insert'
        if len(args) == 1:
            if type(args[0]).__name__ in ["generator", "list", "tuple"]:
                nq.body = args[0]
            else:
                nq.body = [args[0]]
        elif len(args) > 1:
            nq.body = list(args)
        else:
            q = "SynthDB.graph('{}').insert_{}(...).run(c)".format(self.id, iq)
            raise NonExistenceError(q, q+" requires a dictionary or iterable.", '...')
        nq.params['type'] = iq
        if 'conflict' in kwargs:
            nq.params['conflict'] = kwargs['conflict']
        if 'durability' in kwargs:
            nq.params['durability'] = kwargs['durability']
        if len(kwargs) > 0:
            nq.query_string += ".insert_{}({}, {})".format(iq, nq.body, preqlerrors.param_stringer(kwargs))
        else:
            nq.query_string += ".insert_{}({})".format(iq, nq.body)
        return nq
    return f


def make_doc_select_func(cls, b):
    def f(self, obj_id):
        nq = copy(self)
        nq.q = 'pluck'
        nq.params['type'] = b
        nq.params['obj_id'] = obj_id
        nq.stream = False
        nq.query_string += ".{}({})".format(b, preqlerrors.quoted_val(obj_id))
        return nq
    return f


def make_stream_func(cls, b):
    def f(self, *args, **kwargs):
        nq = copy(self)
        nq.q = 'stream'
        nq.params['type'] = b
        nq.stream = True
        if 'index' in kwargs:
            nq.body['index'] = kwargs['index']
        if 'uids' in kwargs and kwargs['uids']:
            nq.body['index'] = 'uid'
        if b == "property_map":
            nq.params['obj_id'] = args[0]
            nq.body['property_map'] = args[0]
            nq.query_string += ".{}('{}')".format(b, args[0])
            return nq
        elif b == "array":
            nq.params['obj_id'] = args[0]
            nq.body['array'] = args[0]
            nq.query_string += ".{}('{}')".format(b, args[0])
            return nq
        if len(args) > 1:
            nq.body['get_all'] = list(set(args))
        elif len(args) == 1:
            if type(args[0]).__name__ in ["generator", "list", "tuple"]:
                nq.body['get_all'] = list(set(args[0]))
            else:
                nq.body['get_all'] = [args[0]]
        nq.query_string += ".{}(".format(b)
        if 'get_all' in nq.body:
            nq.query_string += "{}".format(nq.body['get_all'])
            if len(kwargs) > 0:
                nq.query_string += ", {})".format(preqlerrors.param_stringer(kwargs))
            else:
                nq.query_string += ")"
        else:
            nq.query_string += ")"
        return nq
    return f


def make_topo_func(cls, tq):
    def f(self, **kwargs):
        nq = copy(self)
        if nq.q != "walk":
            nq.q = "topology"
            nq.stream = False
            nq.body['topo_params'] = kwargs
            nq.body['topo'] = tq
        else:
            nq.body['walk_rules']['topo_params'] = kwargs
            nq.body['walk_rules']['topo'] = tq
        ps = preqlerrors.param_stringer(kwargs)
        if ps == "...":
            ps = ''
        nq.query_string += ".{}({})".format(tq, ps)
        return nq
    return f


def make_generation_func(cls, gen_type):
    def f(self, **kwargs):
        nq = copy(self)
        nq.q = "generate"
        nq.body['gen_type'] = gen_type
        nq.body['gen_params'] = kwargs
        ps = preqlerrors.param_stringer(kwargs)
        if ps == '...':
            ps = ''
        nq.query_string += ".{}({})".format(gen_type, ps)
        return nq
    return f


def make_walker_func(cls, w):
    def f(self, **kwargs):
        nq = copy(self)
        nq.q = "walk"
        nq.body['alg'] = w
        nq.body['walk_rules'] = kwargs
        ps = preqlerrors.param_stringer(kwargs)
        nq.query_string += ".{}({})".format(w, ps)
        return nq
    return f


for tt in table_types:
    setattr(PreqlQuery, 'insert_'+tt, make_insert_func(PreqlQuery, tt))
for st in stream_types:
    setattr(PreqlQuery, st, make_stream_func(PreqlQuery, st))
for dt in doc_types:
    setattr(PreqlQuery, dt, make_doc_select_func(PreqlQuery, dt))
for tq in topo_queries:
    setattr(PreqlQuery, tq, make_topo_func(PreqlQuery, tq))
for gf in generator_funcs:
    setattr(create_graph, gf, make_generation_func(create_graph, gf))
for w in walkers:
    setattr(PreqlQuery, w, make_walker_func(PreqlQuery, w))


class load(object):
    def __init__(self, graph_loader, chunk_limit=10000):
        if graph_loader.name is None:
            raise NotImplementedError('Subclasses of GraphLoader require a "name" property.')
        self.graph_loader = graph_loader
        self.nodes = []
        self.links = []
        self.chunk_limit = chunk_limit

    def push_nodes(self, connection):
        d = graph(self.graph_loader.name).insert_nodes(self.nodes).run(connection)
        del self.nodes[:]
        return d

    def push_links(self, connection):
        d = graph(self.graph_loader.name).insert_links(self.links).run(connection)
        del self.links[:]
        return d

    def run(self, connection):
        dt = datetime.now()
        failures = []
        create_graph(self.graph_loader.name).run(connection)
        print "Graph initialized in {}".format(datetime.now()-dt)
        dt = datetime.now()
        d = graph(self.graph_loader.name).insert_node_types(self.graph_loader.node_types()).run(connection)
        if 'failures' in d:
            failures += d['failures']
        print "{} NodeTypes inserted in {}, with {} errors.".format(d['inserted'], datetime.now()-dt, d['errors'])
        dt = datetime.now()
        d = graph(self.graph_loader.name).insert_link_types(self.graph_loader.link_types()).run(connection)
        if 'failures' in d:
            failures += d['failures']
        print "{} LinkTypes inserted in {}, with {} errors.".format(d['inserted'], datetime.now() - dt, d['errors'])
        dt = datetime.now()
        node_inserts = 0
        node_errors = 0
        for n in self.graph_loader.nodes():
            self.nodes.append(n)
            if len(self.nodes) >= self.chunk_limit:
                d = self.push_nodes(connection)
                node_inserts += d['inserted']
                node_errors += d['errors']
                if 'failures' in d:
                    failures += d['failures']
        d = self.push_nodes(connection)
        if 'failures' in d:
            failures += d['failures']
        node_inserts += d['inserted']
        node_errors += d['errors']
        print "{} Nodes inserted in {}, with {} errors.".format(node_inserts, datetime.now() - dt, node_errors)
        dt = datetime.now()
        link_inserts = 0
        link_errors = 0
        for l in self.graph_loader.links():
            self.links.append(l)
            if len(self.links) >= self.chunk_limit:
                d = self.push_links(connection)
                link_inserts += d['inserted']
                link_errors += d['errors']
                if 'failures' in d:
                    failures += d['failures']
        d = self.push_links(connection)
        link_inserts += d['inserted']
        link_errors += d['errors']
        if 'failures' in d:
            failures += d['failures']
        print "{} Links inserted in {}, with {} errors.".format(link_inserts, datetime.now() - dt, link_errors)
        for fail in failures:
            print fail
        return graph(self.graph_loader.name)