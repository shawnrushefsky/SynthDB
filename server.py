# SynthDB - A Fast JSON Graph Database built for billions of edges
# Copyright (C) 2016 Psymphonic LLC
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


# coding = utf-8
import cherrypy
import simplejson as json
import os
import rethinkdb as r
import graph_tool.all as gt
from datetime import datetime
from uuid import uuid4
import re
import itertools
import numpy
import dill as pickle
import operator
from urllib2 import unquote
import synthdb
import preqlerrors
import math
import sys

# Utilities

primary_id_check = re.compile("^(?!\D)\d*_\d*_\d*$|^(?!\D)\d*$")

literal_check = re.compile("^##r.literal\((?P<json_doc>.+?)\)##$", re.U)

path = os.path.dirname(os.path.abspath(__file__))

graphs = {}
property_maps = {}
ndarrays = {}
subgraphs = {}

errors = preqlerrors.errors

free_limits = {'nodes': 1000, 'links': 10000}


def check_key():
    return 'Api-Key' in cherrypy.request.headers and cherrypy.request.headers['Api-Key'] == key


def wrong_key():
    raise cherrypy.HTTPError("403 Forbidden", "You are not allowed to access this resource.")


def auto_reql(q, c, **kwargs):
    try:
        return q.run(c, **kwargs)
    except (r.ReqlDriverError, AttributeError):
        try:
            c.reconnect()
            return q.run(c, **kwargs)
        except (r.ReqlDriverError, AttributeError):
            auto_reql(q, r.connect(timeout=10), **kwargs)


def trim_id(id_val):
    if type(id_val).__name__ in ['str', 'unicode'] and id_val[0] in ['"', "'"]:
        return id_val[1:-1]
    else:
        return id_val


def id_or_uid(o_id, obj_type):
    prim_id = trim_id(o_id)
    if obj_type in ['node', 'link']:
        m = primary_id_check.match(unicode(prim_id))
        if m:
            try:
                obj_id = int(prim_id)
            except ValueError:
                obj_id = prim_id
            uid = False
            id_quote = ''
        else:
            obj_id = prim_id
            uid = True
            id_quote = "'"
    else:
        obj_id = prim_id
        uid = False
        id_quote = "'"
    return obj_id, uid, id_quote


def tab_separate(f, delim='\t'):
    buf = ''
    while True:
        c = unicode(f.read(1))
        if c == '':
            return
        buf += c
        if c == delim:
            yield buf
            buf = ''


def db_id(g_id):
    return graphs[g_id].graph_properties['id']


def get_vertex_id(g_id, n_id, c):
    if primary_id_check.match(unicode(n_id)):
        return int(n_id)
    else:
        prim_id = trim_id(n_id)
        d = auto_reql(r.db(db_id(g_id)).table('nodes').get_all(prim_id, index='uid').coerce_to('array'), c)
        if len(d) > 0:
            return int(d[0]['id'])
        return None


def get_edge_id(g_id, e, c):
    if type(e).__name__ == "Edge":
        return "{}_{}_{}".format(int(e.source()), graphs[g_id].edge_properties['id'][e], int(e.target()))
    prim_id = trim_id(e)
    if primary_id_check.match(unicode(prim_id)):
        return prim_id
    else:
        d = auto_reql(r.db(db_id(g_id)).table('links').get_all(prim_id, index='uid').coerce_to('array'), c)
        if len(d) > 0:
            return d[0]['id']
        return None


def get_field_list(d):
    if isinstance(d, str):
        return [d]
    resp = []
    for k, v in d.iteritems():
        resp.append(k)
        if isinstance(v, dict):
            resp += get_field_list(v)
        elif isinstance(v, str):
            resp.append(v)
    return resp


def update_formatter(fp, value):
    d = {}
    dd = d
    latest = fp[-1]
    for k in fp[:-1]:
        dd = dd.setdefault(k, {})
    dd.setdefault(latest, value)
    return d


def get_edge(g, o, t, eid):
    es = g.edge(o, t, all_edges=True)
    for e in es:
        if g.edge_properties['id'][e] == int(eid):
            return e


def stream_gen(iterable, event_stream, coerce_to='stream', count=False):
    if event_stream:
        delimiter = '\n\n'
        prefix = 'data: '
    else:
        delimiter = "\t"
        prefix = ''
    if count:
        return json.dumps(sum(1 for _ in iterable))
    if hasattr(iterable, '__iter__'):
        if coerce_to == 'stream':
            cherrypy.response.headers['Content-Type'] = 'text/event-stream'

            def stream_it():
                for item in iterable:
                    yield prefix + json.dumps(item) + delimiter
                if event_stream:
                    yield '\nevent: usercloseconnection\ndata: ' + json.dumps("terminate connection") + delimiter
            return stream_it()
        elif coerce_to == "array":
            cherrypy.response.headers['Content-Type'] = 'text/plain'
            return json.dumps(list(iterable))
    else:
        return json.dumps(iterable)


def def_node(v, ntype="Node", uid=None):
    if uid is None:
        uid = str(uuid4())
    return {'id': int(v), 'type': ntype, 'uid': uid}


def def_link(g_id, e, ltype='Link', value=1, uid=None):
    if uid is None:
        uid = str(uuid4())
    return {'id': get_edge_id(g_id, e, None), 'type': ltype, 'uid': uid, 'value': value}


# General Graph Administration


def create_graph(g_name=None, durability='hard', c=None):
    if c is None:
        c = r.connect()
    if g_name is None:
        g_name = str(uuid4())
    g_name = g_name.replace('-', '_')
    if g_name in graphs:
        return {'error': errors['IDDuplicates']['graph'](g_name)}
    try:
        auto_reql(r.db_create(g_name), c)
    except r.ReqlOpFailedError:
        return {'error': errors['IDDuplicates']['graph'](g_name)}
    g = gt.Graph()
    g.graph_properties['id'] = g.new_graph_property('string')
    g.graph_properties['id'] = g_name
    auto_reql(r.db(g_name).table_create('nodes', durability=durability), c)
    auto_reql(r.db(g_name).table_create('links', durability=durability), c)
    auto_reql(r.db(g_name).table_create('node_types', durability=durability), c)
    auto_reql(r.db(g_name).table_create('link_types', durability=durability), c)
    auto_reql(r.db(g_name).table('nodes').index_create('uid'), c)
    auto_reql(r.db(g_name).table('links').index_create('uid'), c)
    auto_reql(r.db(g_name).table('node_types').insert(graph_format['node_types']()), c)
    auto_reql(r.db(g_name).table('link_types').insert(graph_format['link_types']()), c)
    g.edge_properties['id'] = g.new_edge_property('int16_t')
    graphs[g_name] = g
    return {'id': g_name, 'message': "Graph('{}') has been created.".format(g_name)}


def load_graph(g_name, c=None):
    if c is None:
        c = r.connect()
    g = gt.Graph()
    g.graph_properties['id'] = g.new_graph_property('string')
    g.graph_properties['id'] = g_name
    g.edge_properties['id'] = g.new_edge_property('int16_t')
    dt = datetime.now()
    print "\nProcessing Graph('{}')".format(g_name)
    print "    Loading in Nodes..."
    num_nodes = auto_reql(r.db(g_name).table('nodes').count(), c)
    print "    %d Nodes Identified. Populating model..." % num_nodes
    g.add_vertex(n=num_nodes)
    print "    Done."
    print "    Loading in Links..."
    for [o, l_id, t] in auto_reql(r.db(g_name).table('links').map(lambda link: link['id'].split('_')), c):
        e = g.add_edge(o, t)
        g.edge_properties['id'][e] = l_id
    print "    Done.  %d Links Identified." % g.num_edges()
    print ">>>>Graph('{0}') Loaded in {1}".format(g_name, datetime.now()-dt)
    return g


def topo_error(g_id, name, kwargs, params, gen=False):
    forbidden = ['id', 'gen_type', 'type']
    kwargs = {k: kwargs[k] for k in kwargs if k not in forbidden}
    return {'error': errors['MissingFields']['graph_topo'](g_id, name, kwargs, params['missing_req'], gen)}


def topo_format_error(g_id, name, kwargs, params, gen=False):
    forbidden = ['id', 'gen_type', 'type']
    kwargs = {k: kwargs[k] for k in kwargs if k not in forbidden}
    return {'error': errors['SyntaxError']['graph_topo'](g_id, name, kwargs, params['type_error'], gen)}


def convert_fields(g_id, p, float_conversions, bool_conversions, int_conversions, prop_maps):
    params = {}
    for k in p:
        if k in float_conversions:
            try:
                params[k] = float(p[k])
            except ValueError:
                return {'type_error': {'key': k, 'value': p[k], 'correct_type': 'Float'}}
        elif k in bool_conversions:
            try:
                params[k] = bool(p[k])
            except ValueError:
                return {'type_error': {'key': k, 'value': p[k], 'correct_type': 'Boolean'}}
        elif k in int_conversions:
            try:
                params[k] = int(p[k])
            except ValueError:
                return {'type_error': {'key': k, 'value': p[k], 'correct_type': 'Integer'}}
        elif k in prop_maps:
            if p[k] in property_maps[g_id]:
                params[k] = property_maps[g_id][p[k]]
            else:
                return {'type_error': {'key': k, 'value': p[k], 'correct_type': 'PropertyMap'}}
    return params


def prep_pm(g_id):
    if g_id not in property_maps:
        property_maps[g_id] = {}
        ndarrays[g_id] = {}
        subgraphs[g_id] = {}
    return graphs[g_id]


def purge_graph(g_id):
    del graphs[g_id]
    if g_id in property_maps:
        del property_maps[g_id]
        del ndarrays[g_id]
        del subgraphs[g_id]


acceptable_types = ['nodes', 'links', 'node_types', 'link_types']

valid_stream_coerces = ['stream', 'array']


def node_property_map(g_id, prop_map_name, prop_map_type, func, conn):
    g = prep_pm(g_id)
    pm = g.new_vertex_property(prop_map_type)
    if type(func).__name__ in ['str', 'unicode']:
        final_func = r.js("(function(node){return [node['id'], %s(node)]})" % func)
    else:
        def final_func(node):
            return node['id'], func(node)
    for node_id, node_val in auto_reql(r.db(db_id(g_id)).table('nodes').map(final_func), conn):
        pm[node_id] = node_val
    property_maps[g_id][prop_map_name] = pm
    return {'property_map': prop_map_name}


def link_property_map(g_id, prop_map_name, prop_map_type, func, conn):
    g = prep_pm(g_id)
    pm = g.new_edge_property(prop_map_type)
    if type(func).__name__ in ['str', 'unicode']:
        final_func = r.js("(function(link){return [link['id'].split('_'), %s(link)]})" % func)
    else:
        def final_func(link):
            return link['id'].split('_'), func(link)
    for [o, eid, t], link_val in auto_reql(r.db(db_id(g_id)).table('links').map(final_func), conn):
        e = get_edge(g, o, t, eid)
        pm[e] = link_val
    property_maps[g_id][prop_map_name] = pm
    return {'property_map': prop_map_name}


def invalid_float_replacer(val):
    if isinstance(val, float):
        if math.isinf(val):
            return "Inf"
        elif math.isnan(val):
            return "NaN"
        else:
            return val
    elif val == 2147483647:
        return "Inf"
    else:
        return val


# REST API LAYER

def p_b_ot():
    params = json.loads(cherrypy.request.headers['params'])
    body = cherrypy.request.body.read()
    event_stream = False
    try:
        body = json.loads(body)
        if 'js_func' in body and body['js_func']:
            js_func = True
        else:
            js_func = False
        if 'event_stream' in body and body['event_stream']:
            event_stream = True
    except json.JSONDecodeError:
        body = pickle.loads(body)
        js_func = False
    if 'type' in params:
        obj_type = params['type']
    else:
        obj_type = None
    return params, body, obj_type, js_func, event_stream


class API(object):
    def __init__(self):
        self.exposed = True
        self.delim = '\t'
        self.chunk_limit = 200
        with open(os.path.join(path, 'preql_queries.pickle')) as pq:
            self.queries = pickle.load(pq)

    def q(self):
        if not secure or check_key():
            head = cherrypy.request.headers
            q = head['q']
            if q == "ping":
                return "Hi there!"
            elif q == "list_graphs":
                return json.dumps([g_id for g_id in graphs])
            if 'g' not in head:
                return json.dumps({'error': errors['MissingFields']['id']['graph']()})
            else:
                g_id = head['g']
            if q not in ["create_graph", "generate"]:
                if g_id not in graphs:
                    return json.dumps({'error': errors['Nonexistence']['graph'](g_id)})
                dbid = db_id(g_id)
            else:
                dbid = None
            conn = r.connect()
            if q in self.queries:
                return self.queries[q](self, g_id, dbid, head, conn)
        else:
            wrong_key()
    q.exposed = True
    q._cp_config = {'response.stream': True}

    def update(self):
        if not secure or check_key():
            with open(os.path.join(path, 'preql_queries.pickle')) as pq:
                self.queries = pickle.load(pq)
            with open(os.path.join(path, "synthdb_internal.pickle")) as sdb:
                funcs = pickle.load(sdb)
                node_topo_funcs.update(funcs['node_topo_funcs'])
                link_topo_funcs.update(funcs['link_topo_funcs'])
                graph_format.update(funcs['graph_format'])
                removers.update(funcs['removers'])
                graph_tool_functions.update(funcs['graph_tool_functions'])
                graph_generator_functions.update(funcs['graph_generator_functions'])
                walkers.update(funcs['walkers'])
                topo_formats.update(funcs['topo_formats'])
            return json.dumps("Functions updated.")
        else:
            wrong_key()
    update.exposed = True

if __name__ == "__main__":
    cherrypy.config.update(os.path.join(path, "config", "server.conf"))
    try:
        r_conn = r.connect()
    except r.ReqlDriverError:
        raise preqlerrors.NonexistenceError(errors['RethinkDBNonexistence']())
    if "--secure" in sys.argv:
        secure = True
        with open(os.path.join(path, 'secure.key')) as f:
            key = f.read()
            print "KEY\n-------------\n{}\n-------------".format(key)
        cherrypy.config.update({
            "server.socket_host": "0.0.0.0",
            "server.ssl_certificate": os.path.join(path, "synthdb_cert.pem"),
            "server.ssl_private_key": os.path.join(path, "synthdb_key.pem"),
            "server.ssl_module": "pyopenssl",
            "server.socket_port": 443
        })
    else:
        secure = False
    if "--free" in sys.argv:
        free_mode = True
    else:
        free_mode = False
    with open(os.path.join(path, "synthdb_internal.pickle")) as sdb:
        funcs = pickle.load(sdb)
        node_topo_funcs = funcs['node_topo_funcs']
        link_topo_funcs = funcs['link_topo_funcs']
        graph_format = funcs['graph_format']
        removers = funcs['removers']
        graph_tool_functions = funcs['graph_tool_functions']
        graph_generator_functions = funcs['graph_generator_functions']
        walkers = funcs['walkers']
        topo_formats = funcs['topo_formats']

    dbs = auto_reql(r.db_list(), r_conn)
    for n in dbs:
        if n != "test" and n != "rethinkdb":
            temp_g = load_graph(n, r_conn)
            graphs[n] = temp_g

    cherrypy.tree.mount(API(), '/preql')
    cherrypy.engine.timeout_monitor.unsubscribe()
    cherrypy.engine.start()
    cherrypy.engine.block()