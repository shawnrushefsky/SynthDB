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

# Establish Minimum Formatting and Default Values for objects


def format_node_type(*args):
    if len(args) == 0:
        d = {}
    else:
        d = args[1]
    default = {
        'id': 'Node',
        'shape': 'dynamic',
        'color': 'dynamic',
        'image': None
    }
    for k in default:
        if k not in d:
            d[k] = default[k]
    return d


def format_link_type(*args):
    if len(args) == 0:
        d = {}
    else:
        d = args[1]
    default = {
        'id': 'Link',
        'color': 'dynamic',
        'image': None,
        'min': 1,
        'max': 1,
        'function': 'elastic',
        'units': 'none',
    }
    for k in default:
        if k not in d:
            d[k] = default[k]
    return d


def def_node(v, ntype="Node", uid=None):
    if uid is None:
        uid = str(uuid4())
    return {'id': int(v), 'type': ntype, 'uid': uid}


def def_link(g_id, e, ltype='Link', value=1, uid=None):
    if uid is None:
        uid = str(uuid4())
    return {'id': get_edge_id(g_id, e, None), 'type': ltype, 'uid': uid, 'value': value}


def add_node(g_id, node_data, conflict, conn):
    if free_mode and graphs[g_id].num_vertices() >= free_limits['nodes']:
        raise ValueError("This graph has already met it's limit of {} nodes".format(free_limits['nodes']))
    if 'type' not in node_data:
        node_data['type'] = "Node"
    v_id = None
    if conflict in ['replace', 'update'] and ('id' in node_data or 'uid' in node_data):
        if 'id' in node_data and primary_id_check.match(unicode(node_data['id'])):
            v_id = node_data['id']
        elif 'uid' in node_data:
            if node_data['uid'] != '':
                v_id = get_vertex_id(g_id, node_data['uid'], conn)
            else:
                del node_data['uid']
    if v_id is None:
        v_id = int(graphs[g_id].add_vertex())
    node_data['id'] = v_id
    if 'uid' not in node_data:
        node_data['uid'] = str(uuid4())
    return node_data


def add_link(g_id, link_data, conflict, conn):
    if free_mode and graphs[g_id].num_edges() >= free_limits['links']:
        raise ValueError("This graph has already met it's limit of {} links".format(free_limits['links']))
    if 'origin' not in link_data or 'terminus' not in link_data:
        raise ValueError("Links require an 'origin' and 'terminus'.")
    if type(link_data['origin']).__name__ in ['str', 'unicode']:
        link_data['origin'] = unquote(link_data['origin'])
        d = auto_reql(r.db(db_id(g_id)).table('nodes').get_all(link_data['origin'], index='uid')['id'].coerce_to('array'), conn)
        if len(d) == 0:
            raise ValueError('Origin Node with UID %s not found.' % link_data['origin'])
        o = d[0]
    elif type(link_data['origin']).__name__ in ['int']:
        o = link_data['origin']
    else:
        raise TypeError("'origin' field requires an integer(short) for a numerical ID, or a string for uid.")
    if type(link_data['terminus']).__name__ in ['str', 'unicode']:
        link_data['terminus'] = unquote(link_data['terminus'])
        d = auto_reql(r.db(db_id(g_id)).table('nodes').get_all(link_data['terminus'], index='uid')['id'].coerce_to('array'), conn)
        if len(d) == 0:
            raise ValueError('Terminus Node with UID %s not found.' % link_data['terminus'])
        t = d[0]
    elif type(link_data['terminus']).__name__ in ['int']:
        t = link_data['terminus']
    else:
        raise TypeError("'terminus' field requires an integer(short) for a numerical ID, or a string for uid.")
    g = graphs[g_id]
    es = g.edge(o, t, all_edges=True)
    l_id = len(es)
    e = g.add_edge(o, t)
    g.edge_properties['id'][e] = l_id
    link_data['id'] = '{}_{}_{}'.format(o, l_id, t)
    if 'type' not in link_data:
        link_data['type'] = "Link"
    try:
        lt = auto_reql(r.db(db_id(g_id)).table('link_types').get(link_data['type']), conn)
    except r.ReqlNonExistenceError:
        lt = format_link_type({'id': link_data['type']})
        auto_reql(r.db(db_id(g_id)).table('link_types').insert(lt), conn)
    if 'value' not in link_data:
        link_data['value'] = lt['min']
    elif lt['function'] == "elastic":
        change = True
        if link_data['value'] < lt['min']:
            lt['min'] = link_data['value']
        elif link_data['value'] > lt['max']:
            lt['max'] = link_data['value']
        else:
            change = False
        if change:
            auto_reql(r.db(db_id(g_id)).table('link_types').get(lt['id']).update(lt), conn)

    if 'uid' not in link_data:
        link_data['uid'] = str(uuid4())
    del link_data['origin']
    del link_data['terminus']
    return link_data


# Delete functions for all object types


def remove_link(g_id, link_id, c=None):
    if c is None:
        c = r.connect()
    li = [int(v) for v in link_id.split('_')]
    g = graphs[g_id]
    try:
        o = g.vertex(li[0])
        t = g.vertex(li[2])
    except ValueError:
        return {'error': errors['Nonexistence']['link'](g_id, link_id)}
    es = g.edge(o, t, all_edges=True)
    for e in es:
        if g.edge_properties['id'][e] == li[1]:
            g.remove_edge(e)
            auto_reql(r.db(db_id(g_id)).table('links').get(link_id).delete(), c)
            break
    es = g.edge(o, t, all_edges=True)
    updated = {}
    if len(es) > 0:
        inserts = []
        deletes = []
        for i, e in enumerate(es):
            e_id = '{}_{}_{}'.format(int(e.source()), g.edge_properties['id'][e], int(e.target()))
            deletes.append(e_id)
            d = auto_reql(r.db(db_id(g_id)).table('links').get(e_id), c)
            g.edge_properties['id'][e] = i
            d['id'] = '{}_{}_{}'.format(int(e.source()), i, int(e.source()))
            updated[d['uid']] = {'old_id': e_id, 'new_id': d['id']}
            inserts.append(d)
        auto_reql(r.db(db_id(g_id)).table('links').get_all(*deletes).delete(), c)
        auto_reql(r.db(db_id(g_id)).table('links').insert(inserts), c)
    return {'links_deleted': 1, 'links_updated': updated}


def remove_node(g_id, node_id, c=None):
    if c is None:
        c = r.connect()
    g = graphs[g_id]
    dbid = db_id(g_id)

    #  Get the last node in the graph to swap
    swap_old_id = g.num_vertices()-1
    if node_id == swap_old_id:
        try:
            links_to_delete = ['{}_{}_{}'.format(e.source(), g.edge_properties['id'][e], e.target()) for e in
                               g.vertex(node_id).all_edges()]
        except ValueError:
            return {'error': errors['Nonexistence']['node'](g_id, node_id)}
        del_link_uids = auto_reql(r.db(dbid).table('links').get_all(*links_to_delete)['uid'].coerce_to('array'), c)
        g.remove_vertex(node_id)
        auto_reql(r.db(dbid).table('links').get_all(*links_to_delete).delete(), c)
        auto_reql(r.db(dbid).table('nodes').get(node_id).delete(), c)
        return {
            'nodes_deleted': 1,
            'links_deleted': del_link_uids,
            'nodes_updated': {},
            'links_updated': {}
        }

    swap = g.vertex(swap_old_id)
    swap_uid = auto_reql(r.db(dbid).table('nodes').get(swap_old_id)['uid'], c)

    #  The new id for the swapped node, which is the same as the id of the node to delete
    swap_new_id = node_id

    #  Get the links that will need to be deleted, because they are attached to the root node.
    try:
        links_to_delete = ['{}_{}_{}'.format(e.source(), g.edge_properties['id'][e], e.target()) for e in
                       g.vertex(node_id).all_edges()]
    except ValueError:
        return {'error': errors['Nonexistence']['node'](g_id, node_id)}

    del_link_uids = auto_reql(r.db(dbid).table('links').get_all(*links_to_delete)['uid'].coerce_to('array'), c)

    #  Get links that will need to be updated to accommodate the swap
    links_to_update = ['{}_{}_{}'.format(int(l.source()), g.edge_properties['id'][l], int(l.target())) for l in
                       swap.all_edges()]

    #  Delete the vertex to be deleted, and delete the associated links from rethink as well
    g.remove_vertex(node_id, fast=True)
    auto_reql(r.db(dbid).table('links').get_all(*links_to_delete).delete(), c)

    #  Get the document for the swap node
    d = auto_reql(r.db(dbid).table('nodes').get_all(swap_uid, index='uid').coerce_to('array'), c)[0]
    # d = auto_reql(r.db(dbid).table('nodes').get(swap_old_id), c)

    #  Change its id to be the deleted node's id
    d['id'] = swap_new_id

    #  Replace the deleted node document with the swap node document
    auto_reql(r.db(dbid).table('nodes').get(swap_new_id).replace(d), c)

    #  Delete the old swap node document
    auto_reql(r.db(dbid).table('nodes').get(swap_old_id).delete(), c)

    #  Get all the old link documents
    old_ld = auto_reql(r.db(dbid).table('links').get_all(*links_to_update), c)
    updated_links = {}
    for l in old_ld:
        old_l_id = l['id']
        l_i = old_l_id.split('_')
        #  If the swap node was the origin, change the id to reflect the new origin id
        if l_i[0] == swap_old_id:
            l['id'] = '{}_{}_{}'.format(swap_new_id, l_i[1], l_i[2])
        # If the swap node was the terminus, change the id to reflect the new origin id
        elif l_i[2] == swap_old_id:
            l['id'] = '{}_{}_{}'.format(l_i[0], l_i[1], swap_new_id)
        updated_links[l['uid']] = {'old_id': old_l_id, 'new_id': l['id']}
    #  Delete the old link docs, and insert new ones with topologically appropriate ids.
    auto_reql(r.db(db_id(g_id)).table('links').get_all(*links_to_update).delete(), c)
    auto_reql(r.db(db_id(g_id)).table('links').insert(old_ld), c)
    return {
        'nodes_deleted': 1,
        'links_deleted': del_link_uids,
        'nodes_updated': {
            d['uid']: {
                'old_id': swap_old_id,
                'new_id': d['id']
            }
        },
        'links_updated': updated_links
    }


def remove_node_type(g_id, n_type, c=None):
    if c is None:
        c = r.connect()
    if n_type == "Node":
        return {'error': errors['Protected']['node_type'](g_id, n_type)}
    try:
        auto_reql(r.db(db_id(g_id)).table('node_types').get(n_type).delete(), c)
    except r.ReqlNonExistenceError:
        return {'error': errors['Nonexistence']['node_type'](g_id, n_type)}
    info = auto_reql(r.db(db_id(g_id)).table('nodes').filter({'type': n_type}).update({'type': 'Node'}), c)
    return {'node_types_deleted': 1, 'nodes_updated': info['replaced']}


def remove_link_type(g_id, l_type, c=None):
    if c is None:
        c = r.connect()
    if l_type == "Link":
        return {'error': errors['Protected']['link_type'](g_id, l_type)}
    try:
        auto_reql(r.db(db_id(g_id)).table('link_types').get(l_type).delete(), c)
    except r.ReqlNonExistenceError:
        return {'error': errors['Nonexistence']['node_type'](g_id, l_type)}
    info = auto_reql(r.db(db_id(g_id)).table('links').filter({'type': l_type}).update({'type': 'Link'}), c)
    return {'link_types_deleted': 1, 'links_updated': info['replaced']}


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
    auto_reql(r.db(g_name).table('node_types').insert(format_node_type()), c)
    auto_reql(r.db(g_name).table('link_types').insert(format_link_type()), c)
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


# Centrality Functions


def centrality_params(g_id, allowed, required, conn, **kw):
    g = prep_pm(g_id)
    p = {k: kw[k] for k in kw if k in allowed}
    missing = [k for k in required if k not in p]
    if len(missing) > 0:
        return None, None, None, {'missing_req': missing}
    float_conversions = ['damping', 'epsilon', 'alpha']
    bool_conversions = ['ret_iter', 'norm', 'harmonic']
    int_conversions = ['max_iter']
    prop_maps = ['pers', 'weight', 'trust_map', 'beta']
    params = convert_fields(g_id, p, float_conversions, bool_conversions, int_conversions, prop_maps)
    if 'type_error' in params:
        return None, None, None, params
    if 'nprop' in p:
        n_map = p['nprop']
        if p['nprop'] in property_maps[g_id]:
            params['vprop'] = property_maps[g_id][p['nprop']]
    else:
        n_map = str(uuid4())
    if 'lprop' in p:
        l_map = p['lprop']
        if p['lprop'] in property_maps[g_id]:
            params['eprop'] = property_maps[g_id][p['lprop']]
    else:
        l_map = str(uuid4())
    if 'origin' in p:
        src = get_vertex_id(g_id, p['origin'], conn)
        if src is None:
            return None, None, None, {'error': errors['Nonexistence']['node'](g_id, p['origin'])}
        params['source'] = src
    if 'terminus' in p:
        tgt = get_vertex_id(g_id, p['terminus'], conn)
        if tgt is None:
            return None, None, None, {'error': errors['Nonexistence']['node'](g_id, p['terminus'])}
        params['target'] = tgt
    if 'auth_prop' in p:
        n_map = p['auth_prop']
        if p['auth_prop'] in property_maps[g_id]:
            params['xprop'] = property_maps[g_id][p['auth_prop']]
    if 'hub_prop' in p:
        l_map = p['hub_prop']
        if p['hub_prop'] in property_maps[g_id]:
            params['yprop'] = property_maps[g_id][p['hub_prop']]
    return g, n_map, l_map, params


def prep_pm(g_id):
    if g_id not in property_maps:
        property_maps[g_id] = {}
        ndarrays[g_id] = {}
        subgraphs[g_id] = {}
    return graphs[g_id]


def pagerank(g_id, conn, **kwargs):
    allowed = ['damping', 'pers', 'weight', 'nprop', 'epsilon', 'max_iter', 'ret_iter']
    g, n_map, l_map, params = centrality_params(g_id, allowed, [], conn, **kwargs)
    if 'missing_req' in params:
        return topo_error(g_id, 'pagerank', kwargs, params)
    elif 'type_error' in params:
        return topo_format_error(g_id, 'pagerank', kwargs, params)
    elif 'error' in params:
        return params
    if 'vprop' in params:
        params['prop'] = params['vprop']
        del params['vprop']
    pr = gt.graph_tool.centrality.pagerank(g, **params)
    if type(pr).__name__ == "tuple":
        property_maps[g_id][n_map] = pr[0]
        return {'property_map': n_map, 'num_iterations': pr[1]}
    property_maps[g_id][n_map] = pr
    return {'property_map': n_map}


def betweenness(g_id, conn, **kwargs):
    allowed = ['nprop', 'lprop', 'weight', 'norm']
    g, n_map, l_map, params = centrality_params(g_id, allowed, [], conn, **kwargs)
    my_name = "betweenness"
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    vb, eb = gt.graph_tool.centrality.betweenness(g, **params)
    property_maps[g_id][n_map] = vb
    property_maps[g_id][l_map] = eb
    return {'node_betweenness': n_map, 'link_betweenness': l_map}


def closeness(g_id, conn, **kwargs):
    allowed = ['weight', 'origin', 'nprop', 'norm', 'harmonic']
    g, n_map, l_map, params = centrality_params(g_id, allowed, [], conn, **kwargs)
    my_name = 'closeness'
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    cm = gt.graph_tool.centrality.closeness(g, **params)
    property_maps[g_id][n_map] = cm
    return {'property_map': n_map}


def eigenvector(g_id, conn, **kwargs):
    allowed = ['weight', 'nprop', 'epsilon', 'max_iter']
    g, n_map, l_map, params = centrality_params(g_id, allowed, [], conn, **kwargs)
    my_name = 'eigenvector'
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    eig_val, eig_map = gt.graph_tool.centrality.eigenvector(g, **params)
    property_maps[g_id][n_map] = eig_map
    return {'max_eigenvalue': eig_val, 'property_map': n_map}


def katz(g_id, conn, **kwargs):
    allowed = ['weight', 'alpha', 'beta', 'nprop', 'norm', 'epsilon', 'max_iter']
    g, n_map, l_map, params = centrality_params(g_id, allowed, [], conn, **kwargs)
    my_name = 'katz'
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    pm = gt.graph_tool.centrality.katz(g, **params)
    property_maps[g_id][n_map] = pm
    return {'property_map': n_map}


def hits(g_id, conn, **kwargs):
    allowed = ['weight', 'auth_prop', 'hub_prop', 'epsilon', 'max_iter']
    g, auth_map, hub_map, params = centrality_params(g_id, allowed, [], conn, **kwargs)
    my_name = 'hits'
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    try:
        eig, x, y = gt.graph_tool.centrality.hits(g, **params)
    except ZeroDivisionError:
        return {'error': errors['TypeError']['topo'](g_id, my_name, "a graph or subgraph with at least 1 link.")}
    property_maps[g_id][auth_map] = x
    property_maps[g_id][hub_map] = y
    return {'max_eigenvalue': eig, 'authority_map': auth_map, 'hub_map': hub_map}


def central_point_dominance(g_id, conn, **kwargs):
    g = prep_pm(g_id)
    params = {}
    my_name = "central_point_dominance"
    if 'betweenness' in kwargs:
        if kwargs['betweenness'] in property_maps[g_id]:
            params['betweenness'] = property_maps[g_id][kwargs['betweenness']]
        else:
            err = {'type_error': {'key': 'betweenness', 'value': kwargs['betweenness'], 'correct_type': 'PropertyMap'}}
            return {'error': topo_format_error(g_id, my_name, kwargs, err)}
    else:
        return {'error': topo_error(g_id, my_name, kwargs, {'missing_req': ['betweenness']})}
    cp = gt.graph_tool.centrality.central_point_dominance(g, **params)
    return cp


def eigentrust(g_id, conn, **kwargs):
    allowed = ['trust_map', 'nprop', 'norm', 'epsilon', 'max_iter', 'ret_iter']
    g, n_map, l_map, params = centrality_params(g_id, allowed, ['trust_map'], conn, **kwargs)
    my_name = "eigentrust"
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    pm = gt.graph_tool.centrality.eigentrust(g, **params)
    if type(pm).__name__  == "tuple":
        property_maps[g_id][n_map] = pm[0]
        return {'property_map': n_map, 'iterations': pm[1]}
    else:
        property_maps[g_id][n_map] = pm
        return {'property_map': n_map}


def trust_transitivity(g_id, conn, **kwargs):
    allowed = ['trust_map', 'origin', 'terminus', 'nprop']
    g, n_map, l_map, params = centrality_params(g_id, allowed, ['trust_map'], conn, **kwargs)
    my_name = 'trust_transitivity'
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    pm = gt.graph_tool.centrality.trust_transitivity(g, **params)
    if type(pm).__name__ == "float":
        return pm
    property_maps[g_id][n_map] = pm
    return {'property_map': n_map}


# Graph Draw Capabilities


def draw_params(g_id, allowed, required, conn, **kw):
    g = prep_pm(g_id)
    p = {k: kw[k] for k in kw if k in allowed}
    missing = [k for k in required if k not in p]
    if len(missing) > 0:
        return None, None, {'missing_req': missing}
    float_conversions = ['a', 'C', 'd', 'K', 'p', 'r', 'theta', 'scale', 'gamma', 'mu', 'mu_p', 'init_step',
                         'cooling_step', 'epsilon', 'mivs_thres', 'ec_thres']
    bool_conversions = ['adaptive_cooling', 'multilevel', 'weighted_coarse', 'circular', 'grid', 'weighted']
    int_conversions = ['max_level', 'max_iter', 'n_iter', 'dim']
    prop_maps = ['weight', 'node_weight', 'pin', 'groups', 'rel_order']
    params = convert_fields(g_id, p, float_conversions, bool_conversions, int_conversions, prop_maps)
    if 'type_error' in params:
        return None, None, params
    if 'nweight' in p and p['nweight'] in property_maps[g_id]:
        params['vweight'] = property_maps[g_id][p['nweight']]
    if 'lweight' in p and p['lweight'] in property_maps[g_id]:
        params['eweight'] = property_maps[g_id][p['lweight']]
    if 'pos' in p:
        p_name = p['pos']
        if p['pos'] in property_maps[g_id]:
            params['pos'] = property_maps[g_id][p['pos']]
    else:
        p_name = str(uuid4())
    if 'coarse_method' in p:
        params['coarse_method'] = str(p['coarse_method'])
    if 't_range' in p:
        params['t_range'] = tuple(p['t_range'])
    if 'root' in p:
        rt = get_vertex_id(g_id, p['root'], conn)
        if rt is None:
            return None, None, {'error': errors['Nonexistence']['node'](g_id, p['root'])}
        params['root'] = rt
    if 'shape' in p:
        params['shape'] = list(p['shape'])

    return g, p_name, params


def sfdp(g_id, conn, **kwargs):
    allowed = ['nweight', 'lweight', 'pin', 'groups', 'C', 'K', 'p', 'theta', 'max_level', 'gamma', 'mu', 'mu_p',
               'init_step', 'cooling_step', 'adaptive_cooling', 'epsilon', 'max_iter', 'pos', 'multilevel',
               'coarse_method', 'mivs_thresh', 'ec_thresh', 'weighted_coarse']
    g, p_name, params = draw_params(g_id, allowed, [], conn, **kwargs)
    my_name = 'sfdp'
    if 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    pos = gt.graph_tool.draw.sfdp_layout(g, **params)
    property_maps[g_id][p_name] = pos
    return {'position': p_name}


def fruchterman_reingold(g_id, conn, **kwargs):
    allowed = ['weight', 'a', 'r', 'scale', 'circular', 'grid', 't_range', 'n_iter', 'pos']
    g, p_name, params = draw_params(g_id, allowed, [], conn, **kwargs)
    my_name = 'fruchterman_reingold'
    if 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    pos = gt.graph_tool.draw.fruchterman_reingold_layout(g, **params)
    property_maps[g_id][p_name] = pos
    return {'position': p_name}


def arf(g_id, conn, **kwargs):
    allowed = ['weight', 'a', 'd', 'dt', 'epsilon', 'max_iter', 'pos', 'dim']
    g, p_name, params = draw_params(g_id, allowed, [], conn, **kwargs)
    my_name = 'arf'
    if 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    pos = gt.graph_tool.draw.arf_layout(g, **params)
    property_maps[g_id][p_name] = pos
    return {'position': p_name}


def radial_tree(g_id, conn, **kwargs):
    allowed = ['root', 'rel_order', 'weighted', 'node_weight', 'r', 'pos']
    g, p_name, params = draw_params(g_id, allowed, ['root'], conn, **kwargs)
    my_name = 'radial_tree'
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    if 'pos' in params:
        del params['pos']
    pos = gt.graph_tool.draw.radial_tree_layout(g, **params)
    property_maps[g_id][p_name] = pos
    return {'position': p_name}


def random_layout(g_id, conn, **kwargs):
    allowed = ['shape', 'pos', 'dim']
    g, p_name, params = draw_params(g_id, allowed, [], conn, **kwargs)
    my_name = 'random_layout'
    if 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    pos = gt.graph_tool.draw.random_layout(g, **params)
    property_maps[g_id][p_name] = pos
    return {'position': p_name}


# Topology Functions


def topology_format(g_id, allowed, required, conn, **kw):
    g = prep_pm(g_id)
    p = {k: kw[k] for k in kw if k in allowed}
    missing = [k for k in required if k not in p]
    if len(missing) > 0:
        return None, None, {'missing_req': missing}
    prop_maps = ['weights', 'weight', 'order', 'label1', 'node_inv1']
    g2_prop_maps = ['label2', 'node_inv2']
    int_conversions = ['max_dist', 'max_n']
    bool_conversions = ['negative_weights', 'directed', 'dense', 'partition', 'embedding', 'kuratowski', 'heuristic',
                        'minimize', 'high_deg', 'norm', 'isomap', 'induced', 'subgraph']
    name_pms = ['mivs', 'match', 'dist_map', 'color', 'tree_map', 'nprop']
    pm_name = None
    params = convert_fields(g_id, p, [], bool_conversions, int_conversions, [])
    if 'type_error' in params:
        return None, None, params
    if 'g2' in p and p['g2'] in graphs:
        params['g2'] = graphs[p['g2']]
    for k in p:
        if k in prop_maps:
            if p[k] in property_maps[g_id]:
                params[k.replace('node', 'vertex').replace('link', 'edge')] = property_maps[g_id][p[k]]
            else:
                return None, None, {'type_error': {'key': k, 'value': p[k], 'correct_type': 'PropertyMap'}}
        elif k in name_pms:
            pm_name = p[k]
            if p[k] in property_maps[g_id]:
                params[k] = property_maps[g_id][p[k]]
        elif k in g2_prop_maps:
            if p[k] in property_maps[p['g2']]:
                params[k.replace('node', 'vertex')] = property_maps[p['g2']][p[k]]
            else:
                return None, None, {'type_error': {'key': k, 'value': p[k], 'correct_type': 'PropertyMap'}}
    if pm_name is None:
        pm_name = str(uuid4())
    if 'root' in p:
        rt = get_vertex_id(g_id, p['root'], conn)
        if rt is None:
            return None, None, {'error': errors['Nonexistence']['node'](g_id, p['root'])}
        params['root'] = g.vertex(rt)
    if 'origin' in p:
        rt = get_vertex_id(g_id, p['origin'], conn)
        if rt is None:
            return None, None, {'error': errors['Nonexistence']['node'](g_id, p['origin'])}
        params['source'] = g.vertex(rt)
    if 'terminus' in p:
        if type(p).__name__ in ['list', 'tuple', 'generator']:
            vs = []
            for v_id in p['terminus']:
                vid = get_vertex_id(g_id, v_id, conn)
                if vid is None:
                    return None, None, {'error': errors['Nonexistence']['node'](g_id, v_id)}
                vs.append(g.vertex(vid))
            params['target'] = vs
        else:
            tgt = get_vertex_id(g_id, p['terminus'], conn)
            if tgt is None:
                return None, None, {'error': errors['Nonexistence']['node'](g_id, p['terminus'])}
            params['target'] = g.vertex(tgt)
    if 'pred_map' in p:
        try:
            params['source'] = bool(p['pred_map'])
        except ValueError:
            if p['pred_map'] in property_maps[g_id]:
                params['pred_map'] = property_maps[g_id][p['pred_map']]
    if 'sub' in p and p['sub'] in graphs:
        params['sub'] = graphs[p['sub']]
    if 'node_label' in p and p['node_label'][0] in property_maps[p['sub']] and p['node_label'][1] in property_maps[g_id]:
        params['vertex_label'] = (property_maps[p['sub']][p['node_label'][0]], property_maps[g_id][p['node_label'][1]])
    if 'link_label' in p and p['link_label'][0] in property_maps[p['sub']] and p['link_label'][1] in property_maps[g_id]:
        params['edge_label'] = (property_maps[p['sub']][p['link_label'][0]], property_maps[g_id][p['link_label'][1]])
    if 'deg' in p:
        params['deg'] = str(p['deg'])

    return g, pm_name, params


def shortest_distance(g_id, conn, **kwargs):
    allowed = ['origin', 'terminus', 'weights', 'negative_weights', 'max_dist', 'directed', 'dense', 'dist_map', 'pred_map']
    g, pm_name, params = topology_format(g_id, allowed, [], conn, **kwargs)
    my_name = 'shortest_distance'
    if 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    resp = gt.graph_tool.topology.shortest_distance(g, **params)
    if type(resp).__name__ == "int32":
        return int(resp)
    user_resp = {'dist_map': pm_name}
    if type(resp).__name__ == "tuple":
        property_maps[g_id][pm_name] = resp[0]
        user_resp['pred_map'] = resp[1]
    property_maps[g_id][pm_name] = resp
    return user_resp


def shortest_path(g_id, conn, **kwargs):
    uids = 'uids' in kwargs and kwargs['uids']
    allowed = ['origin', 'terminus', 'weights', 'negative_weights', 'pred_map']
    required = ['origin', 'terminus']
    g, pm_name, params = topology_format(g_id, allowed, required, conn, **kwargs)
    my_name = 'shortest_path'
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    vlist, elist = gt.graph_tool.topology.shortest_path(g, **params)
    v_nums = [int(v) for v in vlist]
    e_nums = [get_edge_id(g_id, e, conn) for e in elist]
    if uids:
        vids = auto_reql(r.db(db_id(g_id)).table('nodes').get_all(v_nums).map(
                lambda node: (node['id'], node['uid'])), conn)
        v_ind = {k: v for (k, v) in vids}
        vs = [v_ind[int(k)] for k in vlist]
        eids = auto_reql(r.db(db_id(g_id)).table('links').get_all(e_nums).map(
                lambda link: (link['id'], link['uid'])), conn)
        e_ind = {k: v for (k, v) in eids}
        es = [e_ind[get_edge_id(g_id, e, conn)] for e in elist]
    else:
        vs = v_nums
        es = e_nums
    return {'nodes': vs, 'links': es}


def pseudo_diameter(g_id, conn, **kwargs):
    allowed = ['weights', 'origin']
    g, pm_name, params = topology_format(g_id, allowed, [], conn, **kwargs)
    my_name = "pseudo_diameter"
    if 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    pd, ep = gt.graph_tool.topology.pseudo_diameter(g, **params)
    return {'pseudo_diameter': pd, 'endpoints': [int(v) for v in ep]}


def is_bipartite(g_id, conn, **kwargs):
    allowed = ['partition']
    g, pm_name, params = topology_format(g_id, allowed, [], conn, **kwargs)
    my_name = 'is_bipartite'
    if 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    a = gt.graph_tool.topology.is_bipartite(g, **params)
    if type(a).__name__ == "tuple":
        property_maps[g_id][pm_name] = a[1]
        return {'partition': pm_name, 'is_bipartite': a[0]}
    else:
        return a


def is_planar(g_id, conn, **kwargs):
    allowed = ['embedding', 'kuratowski']
    g, pm_name, params = topology_format(g_id, allowed, [], conn, **kwargs)
    my_name = 'is_planar'
    if 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    a = gt.graph_tool.topology.is_planar(g, **params)
    if type(a).__name__ == "tuple":
        pm_name2 = str(uuid4())
        resp = {'is_planar': a[0]}
        if 'embedding' in params and params['embedding']:
            property_maps[g_id][pm_name] = a[1]
            resp['embedding'] = pm_name
        if 'kuratowski' in params and params['kuratowski']:
            property_maps[g_id][pm_name2] = a[-1]
            resp['kuratowski'] = pm_name2
        return resp
    else:
        return a


def is_DAG(g_id, conn, **kwargs):
    del kwargs
    return gt.graph_tool.topology.is_DAG(graphs[g_id])


def max_cardinality_matching(g_id, conn, **kwargs):
    allowed = ['heuristic', 'weight', 'minimize', 'match']
    g, pm_name, params = topology_format(g_id, allowed, [], conn, **kwargs)
    my_name = "max_cardinality_matching"
    if 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    match = gt.graph_tool.topology.max_cardinality_matching(g, **params)
    if type(match).__name__ == "tuple":
        property_maps[g_id][pm_name] = match[0]
        return {'property_map': pm_name, 'max_link_weight': match[1]}
    else:
        property_maps[g_id][pm_name] = match
        return {'property_map': pm_name}


def max_independent_node_set(g_id, conn, **kwargs):
    allowed = ['high_deg', 'mivs']
    g, pm_name, params = topology_format(g_id, allowed, [], conn, **kwargs)
    my_name = "max_independent_node_set"
    if 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    match = gt.graph_tool.topology.max_independent_vertex_set(g, **params)
    property_maps[g_id][pm_name] = match
    return {'property_map': pm_name}


def link_reciprocity(g_id, conn, **kwargs):
    del kwargs
    return gt.graph_tool.topology.edge_reciprocity(graphs[g_id])


def tsp_tour(g_id, conn, **kwargs):
    allowed = ['origin', 'weight']
    g, pm_name, params = topology_format(g_id, allowed, ['origin'], conn, **kwargs)
    my_name = 'tsp_tour'
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    if 'source' in params:
        params['src'] = params['source']
        del params['source']
    g.set_directed(False)
    try:
        tour = gt.graph_tool.topology.tsp_tour(g, **params)
    except ValueError, msg:
        print msg
        msg = str(msg)
        if "The graph may not contain an edge with negative weight" in msg:
            req = "all Link weights to be positive."
        else:
            req = msg
        return {'error': errors['TypeError']['topo'](g_id, my_name, req)}
    g.set_directed(True)
    ndarrays[g_id][pm_name] = tour
    return {'tour': pm_name}


def sequential_node_coloring(g_id, conn, **kwargs):
    allowed = ['order', 'color']
    g, pm_name, params = topology_format(g_id, allowed, [], conn, **kwargs)
    my_name = "sequential_node_coloring"
    if 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    color = gt.graph_tool.topology.sequential_vertex_coloring(g, **params)
    property_maps[g_id][pm_name] = color
    return {'property_map': pm_name}


def similarity(g_id, conn, **kwargs):
    allowed = ['g2', 'label1', 'label2', 'norm']
    g, pm_name, params = topology_format(g_id, allowed, ['g2'], conn, **kwargs)
    my_name = "similarity"
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    return gt.graph_tool.topology.similarity(g, **params)


def isomorphism(g_id, conn, **kwargs):
    allowed = ['g2', 'node_inv1', 'node_inv2', 'isomap']
    g, pm_name, params = topology_format(g_id, allowed, ['g2'], conn, **kwargs)
    my_name = 'isomorphism'
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    a = gt.graph_tool.topology.isomorphism(g, **kwargs)
    if type(a).__name__ == "tuple":
        property_maps[g_id][pm_name] = a[1]
        return {'is_isomorphic': a[0], 'isomap': pm_name}
    else:
        return a


def subgraph_isomorphism(g_id, conn, **kwargs):
    allowed = ['sub', 'max_n', 'node_label', 'link_label', 'induced', 'subgraph']
    required = ['sub']
    g, pm_name, params = topology_format(g_id, allowed, required, conn, **kwargs)
    my_name = 'subgraph_isomorphism'
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    params['g'] = g
    vmaps = gt.graph_tool.topology.subgraph_isomorphism(**params)
    resp = []
    for m in vmaps:
        m_id = str(uuid4())
        property_maps[g_id][m_id] = m
        resp.append(m_id)
    return {'property_maps': resp}


def min_spanning_tree(g_id, conn, **kwargs):
    allowed = ['weights', 'root', 'tree_map']
    required = []
    g, pm_name, params = topology_format(g_id, allowed, required, conn, **kwargs)
    my_name = "min_spanning_tree"
    if 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    try:
        tmap = gt.graph_tool.topology.min_spanning_tree(g, **params)
    except ValueError, msg:
        print msg
        msg = str(msg)
        if "The graph may not contain an edge with negative weight" in msg:
            req = "all Link weights to be positive."
        else:
            req = msg
        return {'error': errors['TypeError']['topo'](g_id, my_name, req)}
    property_maps[g_id][pm_name] = tmap
    return {'property_map': pm_name}


def random_spanning_tree(g_id, conn, **kwargs):
    allowed = ['weights', 'root', 'tree_map']
    g, pm_name, params = topology_format(g_id, allowed, [], conn, **kwargs)
    my_name = "random_spanning_tree"
    if 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    try:
        g.set_directed(False)
        tmap = gt.graph_tool.topology.random_spanning_tree(g, **params)
        g.set_directed(True)
    except ValueError, msg:
        msg = str(msg)
        print msg
        if "There must be a path from all vertices to the root vertex" in msg:
            rt = msg.split(": ")[1]
            req = "a valid path from all Nodes to the root Node: {}".format(rt)
        else:
            req = msg
        return {'error': errors['TypeError']['topo'](g_id, my_name, req)}
    property_maps[g_id][pm_name] = tmap
    return {'property_map': pm_name}


def dominator_tree(g_id, conn, **kwargs):
    allowed = ['root', 'dom_map']
    g, pm_name, params = topology_format(g_id, allowed, ['root'], conn, **kwargs)
    my_name = "dominator_tree"
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    try:
        g.set_directed(True)
        dmap = gt.graph_tool.topology.dominator_tree(g, **params)
    except ValueError, msg:
        print msg
        msg = str(msg)
        if 'requires a directed graph' in msg:
            req = "a Directed Graph."
        else:
            req = msg
        return {'error': errors['TypeError']['topo'](g_id, my_name, req)}
    property_maps[g_id][pm_name] = dmap
    return {'property_map': pm_name}


def topological_sort(g_id, conn, **kwargs):
    del kwargs
    g = prep_pm(g_id)
    my_name = "topological_sort"
    try:
        tsort = gt.graph_tool.topology.topological_sort(g)
    except ValueError, msg:
        print msg
        msg = str(msg)
        return {'error': errors['TypeError']['topo'](g_id, my_name, "a Directed Acyclic Graph (DAG)")}
    l_id = str(uuid4())
    ndarrays[g_id][l_id] = tsort
    return {'sort': l_id}


def transitive_closure(g_id, conn, **kwargs):
    del kwargs
    g = prep_pm(g_id)
    tc = gt.graph_tool.topology.transitive_closure(g)
    s_id = str(uuid4()).replace('-', '_')
    graphs[s_id] = tc
    tc.graph_properties['id'] = tc.new_graph_property('string')
    tc.graph_properties['id'] = g_id
    return {'transitive_closure': s_id}


def kcore_decomposition(g_id, conn, **kwargs):
    allowed = ['deg', 'nprop']
    g, pm_name, params = topology_format(g_id, allowed, [], conn, **kwargs)
    my_name = "kcore_decomposition"
    if 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params)
    elif 'error' in params:
        return params
    if 'nprop' in params:
        params['vprop'] = params['nprop']
        del params['nprop']
    km = gt.graph_tool.topology.kcore_decomposition(g, **params)
    property_maps[g_id][pm_name] = km
    return {'property_map': pm_name}


# Generator Functions


def generator_format(allowed, required, **kw):
    p = {k: kw[k] for k in kw if k in allowed}
    missing = [k for k in required if k not in p]
    if len(missing) > 0:
        return {'missing_req': missing}
    bool_vals = ['directed', 'parallel_links', 'self_loops', 'degree_block', 'random', 'periodic']
    int_vals = ['N', 'd', 'k', 'm']
    float_vals = ['radius', 'c', 'gamma']
    str_vals = ['block_type', 'type']
    list_vals = ['shape', 'ranges']
    params = convert_fields(None, p, float_vals, [], int_vals, [])
    if 'type_error' in params:
        return params
    for k in p:
        if k in bool_vals:
            params[k.replace('links', 'edges')] = bool(p[k])
        elif k in str_vals:
            try:
                params[k] = str(p[k])
            except ValueError:
                return {'type_error': {'key': k, 'value': p[k], 'correct_type': 'String'}}
        elif k in list_vals:
            try:
                params[k] = list(p[k])
            except ValueError:
                return {'type_error': {'key': k, 'value': p[k], 'correct_type': 'Array'}}
    if 'deg_sampler' in p:
        try:
            tuple(p['deg_sampler'])
        except ValueError:
            return {'type_error': {'key': 'deg_sampler', 'value': p['deg_sampler'], 'correct_type': 'Array'}}

        def f():
            return tuple(p['deg_sampler'])
        params['deg_sampler'] = f
    if 'seed_graph' in p and p['seed_graph'] in graphs:
        params['seed_graph'] = graphs[p['seed_graph']]
    return params


def finalize_graph(g, g_id, conn, durability='hard'):
    g.graph_properties['id'] = g.new_graph_property('string')
    g.graph_properties['id'] = g_id
    g.edge_properties['id'] = g.new_edge_property('int16_t')
    if g_id in graphs:
        return {'error': errors['IDDuplicates']['graph'](g_id)}
    try:
        auto_reql(r.db_create(g_id), conn)
    except r.ReqlOpFailedError:
        return {'error':errors['IDDuplicates']['graph'](g_id)}
    graphs[g_id] = g
    prep_pm(g_id)
    auto_reql(r.db(db_id(g_id)).table_create('nodes', durability=durability), conn)
    auto_reql(r.db(db_id(g_id)).table_create('links', durability=durability), conn)
    auto_reql(r.db(db_id(g_id)).table_create('node_types', durability=durability), conn)
    auto_reql(r.db(db_id(g_id)).table_create('link_types', durability=durability), conn)
    auto_reql(r.db(db_id(g_id)).table('nodes').index_create('uid'), conn)
    auto_reql(r.db(db_id(g_id)).table('links').index_create('uid'), conn)
    auto_reql(r.db(db_id(g_id)).table('node_types').insert(format_node_type()), conn)
    auto_reql(r.db(db_id(g_id)).table('link_types').insert(format_link_type()), conn)
    g.set_directed(True)
    nodes = []
    links = []
    for v in g.vertices():
        nodes.append(def_node(v))
        if len(nodes) >= 200:
            auto_reql(r.db(db_id(g_id)).table('nodes').insert(nodes), conn)
            del nodes[:]
    auto_reql(r.db(db_id(g_id)).table('nodes').insert(nodes), conn)
    del nodes[:]
    for e in g.edges():
        g.edge_properties['id'][e] = g.edge(e.source(), e.target(), all_edges=True).index(e)
        links.append(def_link(g_id, e))
        if len(links) >= 200:
            auto_reql(r.db(db_id(g_id)).table('links').insert(links), conn)
            del links[:]
    auto_reql(r.db(db_id(g_id)).table('links').insert(links), conn)
    del links[:]
    graphs[g_id] = g
    return {}


def random_graph(g_id, conn, durability='hard', **kwargs):
    allowed = ['N', 'deg_sampler', 'directed', 'parallel_links', 'self_loops', 'block_membership', 'block_type',
               'degree_block', 'random']
    required = ['N', 'deg_sampler']
    params = generator_format(allowed, required, **kwargs)
    my_name = "random_graph"
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params, True)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params, True)
    elif 'error' in params:
        return params
    g = gt.graph_tool.generation.random_graph(**params)
    resp = finalize_graph(g, g_id, conn, durability=durability)
    if 'error' in resp:
        return resp
    return {'id': g_id, 'nodes': g.num_vertices(), 'links': g.num_edges()}


def triangulation(g_id, conn, durability='hard', **kwargs):
    allowed = ['N', 'd', 'type', 'periodic']
    required = ['N', 'd']
    params = generator_format(allowed, required, **kwargs)
    my_name = 'triangulation'
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params, True)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params, True)
    elif 'error' in params:
        return params
    points = numpy.random.random((params['N'], params['d']))
    del params['N']
    del params['d']
    params['points'] = points
    g, pos = gt.graph_tool.generation.triangulation(**params)
    resp = finalize_graph(g, g_id, conn, durability=durability)
    if 'error' in resp:
        return resp
    pmap = str(uuid4())
    property_maps[g_id][pmap] = pos
    return {'id': g_id, 'nodes': g.num_vertices(), 'links': g.num_edges(), 'position': pmap}


def lattice(g_id, conn, durability='hard', **kwargs):
    allowed = ['shape', 'periodic']
    required = ['shape']
    params = generator_format(allowed, required, **kwargs)
    my_name = 'lattice'
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params, True)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params, True)
    elif 'error' in params:
        return params
    g = gt.graph_tool.generation.lattice(**params)
    resp = finalize_graph(g, g_id, conn, durability=durability)
    if 'error' in resp:
        return resp
    return {'id': g_id, 'nodes': g.num_vertices(), 'links': g.num_edges()}


def complete_graph(g_id, conn, durability='hard', **kwargs):
    allowed = ['N', 'self_loops', 'directed']
    required = ['N']
    params = generator_format(allowed, required, **kwargs)
    my_name = 'complete_graph'
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params, True)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params, True)
    elif 'error' in params:
        return params
    g = gt.graph_tool.generation.complete_graph(**params)
    resp = finalize_graph(g, g_id, conn, durability=durability)
    if 'error' in resp:
        return resp
    return {'id': g_id, 'nodes': g.num_vertices(), 'links': g.num_edges()}


def circular_graph(g_id, conn, durability='hard', **kwargs):
    allowed = ['N', 'k', 'self_loops', 'directed']
    required = ['N']
    params = generator_format(allowed, required, **kwargs)
    my_name = 'circular_graph'
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params, True)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params, True)
    elif 'error' in params:
        return params
    g = gt.graph_tool.generation.circular_graph(**params)
    resp = finalize_graph(g, g_id, conn, durability=durability)
    if 'error' in resp:
        return resp
    return {'id': g_id, 'nodes': g.num_vertices(), 'links': g.num_edges()}


def geometric_graph(g_id, conn, durability='hard', **kwargs):
    allowed = ['N', 'd', 'radius', 'ranges']
    required = ['N', 'd', 'radius']
    params = generator_format(allowed, required, **kwargs)
    my_name = 'geometric_graph'
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params, True)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params, True)
    elif 'error' in params:
        return params
    points = numpy.random.random((params['N'], params['d']))
    del params['N']
    del params['d']
    params['points'] = points
    g, pos = gt.graph_tool.generation.geometric_graph(**params)
    resp = finalize_graph(g, g_id, conn, durability=durability)
    if 'error' in resp:
        return resp
    pmap = str(uuid4())
    property_maps[g_id][pmap] = pos
    return {'id': g_id, 'nodes': g.num_vertices(), 'links': g.num_edges(), 'position': pmap}


def price_network(g_id, conn, durability='hard', **kwargs):
    allowed = ['N', 'm', 'c', 'gamma', 'directed', 'seed_graph']
    required = ['N']
    params = generator_format(allowed, required, **kwargs)
    my_name = 'price_network'
    if 'missing_req' in params:
        return topo_error(g_id, my_name, kwargs, params, True)
    elif 'type_error' in params:
        return topo_format_error(g_id, my_name, kwargs, params, True)
    elif 'error' in params:
        return params
    g = gt.graph_tool.generation.price_network(**params)
    resp = finalize_graph(g, g_id, conn, durability=durability)
    if 'error' in resp:
        return resp
    return {'id': g_id, 'nodes': g.num_vertices(), 'links': g.num_edges()}


# Basic Topo Functions

def all_links(g_id, n_id, conn, **kwargs):
    node_id = get_vertex_id(g_id, n_id, conn)
    if node_id is None:
        return {'error': errors['Nonexistence']['node'](g_id, n_id)}
    link_ids = [get_edge_id(g_id, e, conn) for e in graphs[g_id].vertex(node_id).all_edges()]
    if len(link_ids) > 0:
        if 'filter' in kwargs:
            return auto_reql(r.db(db_id(g_id)).table('links').get_all(*link_ids).filter(kwargs['filter']).coerce_to('array'), conn)
        else:
            return auto_reql(r.db(db_id(g_id)).table('links').get_all(*link_ids).coerce_to('array'), conn)
    else:
        return []


def out_links(g_id, n_id, conn, **kwargs):
    node_id = get_vertex_id(g_id, n_id, conn)
    if node_id is None:
        return {'error': errors['Nonexistence']['node'](g_id, n_id)}
    link_ids = [get_edge_id(g_id, e, conn) for e in graphs[g_id].vertex(node_id).out_edges()]
    if len(link_ids) > 0:
        if 'filter' in kwargs:
            return auto_reql(
                r.db(db_id(g_id)).table('links').get_all(*link_ids).filter(kwargs['filter']).coerce_to('array'), conn)
        else:
            return auto_reql(r.db(db_id(g_id)).table('links').get_all(*link_ids).coerce_to('array'), conn)
    else:
        return []


def in_links(g_id, n_id, conn, **kwargs):
    node_id = get_vertex_id(g_id, n_id, conn)
    if node_id is None:
        return {'error': errors['Nonexistence']['node'](g_id, n_id)}
    link_ids = [get_edge_id(g_id, e, conn) for e in graphs[g_id].vertex(node_id).in_edges()]
    if len(link_ids) > 0:
        if 'filter' in kwargs:
            return auto_reql(
                r.db(db_id(g_id)).table('links').get_all(*link_ids).filter(kwargs['filter']).coerce_to('array'), conn)
        else:
            return auto_reql(r.db(db_id(g_id)).table('links').get_all(*link_ids).coerce_to('array'), conn)
    else:
        return []


def all_neighbors(g_id, n_id, conn, **kwargs):
    node_id = get_vertex_id(g_id, n_id, conn)
    if node_id is None:
        return {'error': errors['Nonexistence']['node'](g_id, n_id)}
    if 'links' in kwargs:
        link_ids = [get_edge_id(g_id, e, conn) for e in graphs[g_id].vertex(node_id).all_edges()]
        node_ids = []
        if len(link_ids) > 0:
            d = auto_reql(r.db(db_id(g_id)).table('links').get_all(*link_ids).filter(kwargs['links'])['id'].coerce_to('array'), conn)
            for v in d:
                o, e, t = v.split('_')
                if int(o) == node_id:
                    node_ids.append(int(t))
                elif int(t) == node_id:
                    node_ids.append(int(o))
    else:
        node_ids = [int(v) for v in graphs[g_id].vertex(node_id).all_neighbours()]
    if len(node_ids) > 0:
        if 'filter' in kwargs:
            filt = kwargs['filter']
        elif 'nodes' in kwargs:
            filt = kwargs['nodes']
        else:
            filt = None
        if filt:
            return auto_reql(r.db(db_id(g_id)).table('nodes').get_all(*node_ids).filter(filt).coerce_to('array'), conn)
        else:
            return auto_reql(r.db(db_id(g_id)).table('nodes').get_all(*node_ids).coerce_to('array'), conn)
    else:
        return []


def in_degree(g_id, n_id, conn, **kwargs):
    node_id = get_vertex_id(g_id, n_id, conn)
    if node_id is None:
        return {'error': errors['Nonexistence']['node'](g_id, n_id)}
    return graphs[g_id].vertex(node_id).in_degree()


def out_degree(g_id, n_id, conn, **kwargs):
    node_id = get_vertex_id(g_id, n_id, conn)
    if node_id is None:
        return {'error': errors['Nonexistence']['node'](g_id, n_id)}
    return graphs[g_id].vertex(node_id).out_degree()


def in_neighbors(g_id, n_id, conn, **kwargs):
    node_id = get_vertex_id(g_id, n_id, conn)
    if node_id is None:
        return {'error': errors['Nonexistence']['node'](g_id, n_id)}
    if 'links' in kwargs:
        link_ids = [get_edge_id(g_id, e, conn) for e in graphs[g_id].vertex(node_id).in_edges()]
        node_ids = []
        if len(link_ids) > 0:
            d = auto_reql(
                r.db(db_id(g_id)).table('links').get_all(*link_ids).filter(kwargs['links'])['id'].coerce_to('array'),
                conn)
            for v in d:
                o, e, t = v.split('_')
                node_ids.append(int(o))
    else:
        node_ids = [int(v) for v in graphs[g_id].vertex(node_id).in_neighbours()]
    if len(node_ids) > 0:
        if 'filter' in kwargs:
            filt = kwargs['filter']
        elif 'nodes' in kwargs:
            filt = kwargs['nodes']
        else:
            filt = None
        if filt:
            return auto_reql(r.db(db_id(g_id)).table('nodes').get_all(*node_ids).filter(filt).coerce_to('array'), conn)
        else:
            return auto_reql(r.db(db_id(g_id)).table('nodes').get_all(*node_ids).coerce_to('array'), conn)
    else:
        return []


def out_neighbors(g_id, n_id, conn, **kwargs):
    node_id = get_vertex_id(g_id, n_id, conn)
    if node_id is None:
        return {'error': errors['Nonexistence']['node'](g_id, n_id)}
    if 'links' in kwargs:
        link_ids = [get_edge_id(g_id, e, conn) for e in graphs[g_id].vertex(node_id).out_edges()]
        node_ids = []
        if len(link_ids) > 0:
            d = auto_reql(
                r.db(db_id(g_id)).table('links').get_all(*link_ids).filter(kwargs['links'])['id'].coerce_to('array'),
                conn)
            for v in d:
                o, e, t = v.split('_')
                node_ids.append(int(t))
    else:
        node_ids = [int(v) for v in graphs[g_id].vertex(node_id).out_neighbours()]
    if len(node_ids) > 0:
        if 'filter' in kwargs:
            filt = kwargs['filter']
        elif 'nodes' in kwargs:
            filt = kwargs['nodes']
        else:
            filt = None
        if filt:
            return auto_reql(r.db(db_id(g_id)).table('nodes').get_all(*node_ids).filter(filt).coerce_to('array'), conn)
        else:
            return auto_reql(r.db(db_id(g_id)).table('nodes').get_all(*node_ids).coerce_to('array'), conn)
    else:
        return []


def origin(g_id, l_id, conn, **kwargs):
    link_id = get_edge_id(g_id, l_id, conn)
    if link_id is None:
        return {'error': errors['Nonexistence']['link'](g_id, l_id)}
    return auto_reql(r.db(db_id(g_id)).table('nodes').get(int(link_id.split('_')[0])), conn)


def terminus(g_id, l_id, conn, **kwargs):
    link_id = get_edge_id(g_id, l_id, conn)
    if link_id is None:
        return {'error': errors['Nonexistence']['link'](g_id, l_id)}
    return auto_reql(r.db(db_id(g_id)).table('nodes').get(int(link_id.split('_')[2])), conn)


def connected_to(g_id, node_list, direction="out", uids=False, c=None, **kwargs):
    if c is None:
        c = r.connect()
    g = prep_pm(g_id)
    if uids:
        cursor = auto_reql(r.db(db_id(g_id)).table('nodes').get_all(*node_list, index='uid')['id'], c)
        root = cursor.next()
    else:
        cursor = node_list
        root = cursor.pop(0)

    out = direction == "in"

    def n_overlap(source_set, target_vertex):
        if out:
            return set(int(v) for v in target_vertex.out_neighbours() if int(v) in source_set)
        else:
            return set(int(v) for v in target_vertex.in_neighbours() if int(v) in source_set)
    if out:
        overlap = set(int(v) for v in g.vertex(root).out_neighbours())
    else:
        overlap = set(int(v) for v in g.vertex(root).in_neighbours())

    for n_id in cursor:
        overlap = n_overlap(overlap, g.vertex(n_id))
        if len(overlap) == 0:
            return []
    overlap = list(overlap)
    if uids:
        return auto_reql(r.db(db_id(g_id)).table('nodes').get_all(*overlap)['uid'], c)
    else:
        return overlap


# Walking functions

def bfs(g_id, o, distance, conn, node_rules=None, link_rules=None):
    g = graphs[g_id]
    n = get_vertex_id(g_id, o, conn)

    def f(max_d):
        dist = g.new_vertex_property('int')
        print dist
        for e in gt.graph_tool.search.bfs_iterator(g, n):
            dist[e.target()] = dist[e.source()] + 1
            if dist[e.target()] > max_d:
                break
            else:
                yield e

    return f(distance)


def purge_graph(g_id):
    del graphs[g_id]
    if g_id in property_maps:
        del property_maps[g_id]
        del ndarrays[g_id]
        del subgraphs[g_id]


def clone_bfs2(g_id, node_id, conn, **kwargs):
    n_id = get_vertex_id(g_id, node_id, conn)
    if n_id is None:
        return {'error': errors['Nonexistence']['node'](g_id, node_id)}
    g1 = prep_pm(g_id)
    node_map = {}
    discovered_nodes = [[n_id], []]
    graph2 = gt.graph_tool.Graph()
    v_id = graph2.new_vertex_property('int')
    if 'direction' not in kwargs:
        kwargs['direction'] = 'out'
    if 'dist' not in kwargs:
        kwargs['dist'] = 1
    if 'filters' not in kwargs:
        filters = None
    else:
        filters = kwargs['filters']

    def one_node(n, lvl):
        out = kwargs['direction'] == "out" \
              or (type(kwargs['direction']).__name__ == "list" and kwargs['direction'][lvl - 1] == "out")
        if filters is None or 'link' not in filters[lvl-1]:
            if out:
                discovered_nodes[lvl] += [int(v) for v in g1.vertex(n).out_neighbours() if int(v) != n_id]
            else:
                discovered_nodes[lvl] += [int(v) for v in g1.vertex(n).in_neighbours() if int(v) != n_id]
        else:
            if out:
                l_ids = [get_edge_id(g_id, e, conn) for e in g1.vertex(n).out_edges()]
                r_query = auto_reql(r.db(db_id(g_id)).table('links').get_all(*l_ids).filter(filters[lvl-1]['link']).map(
                    lambda l: l['id'].split('_')), conn)
                discovered_nodes[lvl] += [int(t) for o, e, t in r_query]
            else:
                l_ids = [get_edge_id(g_id, e, conn) for e in g1.vertex(n).in_edges()]
                r_query = auto_reql(r.db(db_id(g_id)).table('links').get_all(*l_ids).filter(filters[lvl-1]['link']).map(
                    lambda l: l['id'].split('_')), conn)
                discovered_nodes[lvl] += [int(o) for o, e, t in r_query]

    def nfilt_tier(lvl):
        if filters is not None and 'node' in filters[lvl - 1]:
            filtered_node_ids = auto_reql(
                r.db(db_id(g_id)).table('nodes').get_all(*discovered_nodes[lvl]).filter(filters[lvl - 1]['node']).map(
                    lambda node: node['id']).coerce_to('array'), conn)
            discovered_nodes[lvl] = [int(v) for v in filtered_node_ids]

    one_node(n_id, 1)
    nfilt_tier(1)
    if kwargs['dist'] > 1:
        for i in range(1, kwargs['dist']):
            if len(discovered_nodes) > i and len(discovered_nodes[i]) > 0:
                discovered_nodes.append([])
                map(one_node, discovered_nodes[i], (i + 1 for nd in discovered_nodes[i]))
                discovered_nodes[i+1] = list(set(discovered_nodes[i+1]))
                nfilt_tier(i+1)

    if kwargs['topo'] != "similarity":
        def get_all_links():
            for lvl in discovered_nodes:
                for node in lvl:
                    v = graph2.add_vertex()
                    vid = int(node)
                    v_id[v] = vid
                    node_map[vid] = v
                    for e in g1.vertex(vid).out_edges():
                        yield get_edge_id(g_id, e, conn)

        def do_it(link):
            [o, eid, t] = [int(val) for val in link.split('_')]
            try:
                e = graph2.add_edge(node_map[o], node_map[t])
            except KeyError:
                pass

        map(do_it, set(get_all_links()))
        graph2_id = str(uuid4()).replace('-', '_')
        graphs[graph2_id] = graph2
        if kwargs['topo'] == "hits":
            walk_params = {k: kwargs[k] for k in kwargs if k in ['dist', 'direction', 'filters']}
            extra = "Try hits_hub or hits_authority instead."
            return {'error': errors['SyntaxError']['subgraph'](g_id, node_id, 'breadth_first', walk_params,
                                                               kwargs['topo'], kwargs['topo_params'], extra)}
        pm = graph_tool_functions[kwargs['topo']](graph2_id, conn, **kwargs['topo_params'])
        if 'error' in pm:
            purge_graph(graph2_id)
            return pm
        elif 'property_map' in pm:
            pm = pm['property_map']
        elif 'node_betweenness' in pm:
            pm = pm['node_betweenness']
        elif kwargs['topo'] == 'hits_authority':
            pm = pm['authority_map']
        elif kwargs['topo'] == 'hits_hub':
            pm = pm['hub_map']
        elif 'position' in pm:
            pm = pm['position']
        else:
            print pm
        pm = property_maps[graph2_id][pm]
        if 'vector' in pm.value_type():
            d = {str(v_id[v]): [invalid_float_replacer(sv) for sv in pm[v]] for v in graph2.vertices()}
        else:
            d = {str(v_id[v]): invalid_float_replacer(pm[v]) for v in graph2.vertices()}
        purge_graph(graph2_id)
        if 'sort' in kwargs:
            if 'limit' in kwargs:
                return sorted(d.items(), key=operator.itemgetter(1), **kwargs['sort'])[:kwargs['limit']]
            else:
                return sorted(d.items(), key=operator.itemgetter(1), **kwargs['sort'])
        return d
    else:
        if 'direction' not in kwargs['topo_params']:
            sim_dir = "out"
        else:
            sim_dir = kwargs['topo_params']['direction']

        def all_discovered():
            for lvl in discovered_nodes[1:]:
                for node_id in lvl:
                    yield g1.vertex(node_id)

        all_nodes = set(all_discovered())

        def n_overlap(source_set, target_vertex):
            if sim_dir == "out":
                return set(int(v) for v in target_vertex.out_neighbours() if int(v) in source_set)
            elif sim_dir == "in":
                return set(int(v) for v in target_vertex.in_neighbours() if int(v) in source_set)
            else:
                return set(int(v) for v in target_vertex.all_neighbours() if int(v) in source_set)

        if sim_dir == "out":
            o_set = set(int(v) for v in g1.vertex(n_id).out_neighbours() if v in all_nodes)
        elif sim_dir == "in":
            o_set = set(int(v) for v in g1.vertex(n_id).in_neighbours() if v in all_nodes)
        else:
            o_set = set(int(v) for v in g1.vertex(n_id).all_neighbours() if v in all_nodes)

        raw_overlaps = {str(d_node): list(n_overlap(o_set, d_node)) for d_node in all_nodes}
        uid_ref = None
        if 'uids' in kwargs['topo_params'] and kwargs['topo_params']['uids']:
            id_ref_q = auto_reql(r.db(db_id(g_id)).table('nodes').get_all(*[int(v) for v in all_nodes]).map(
                lambda n: [n['id'], n['uid']]), conn)
            uid_ref = {str(v[0]): v[1] for v in id_ref_q}
        if 'nmap' in kwargs:
            if 'js_func' in kwargs and kwargs['js_func']:
                map_ref_q = auto_reql(r.db(db_id(g_id)).table('nodes').get_all(*[int(v) for v in all_nodes]).map(
                    r.js(kwargs['nmap'])), conn)
            else:
                map_ref_q = auto_reql(r.db(db_id(g_id)).table('nodes').get_all(*[int(v) for v in all_nodes]).map(
                    lambda n: [n['id'], kwargs['nmap'](n)]), conn)
            map_ref = {str(v[0]): v[1] for v in map_ref_q}
            if uid_ref is None:
                if 'reduce' in kwargs:
                    if 'js_func' in kwargs and kwargs['js_func']:
                        def js_reducer(input_list):
                            return r.expr(input_list).reduce(r.js(kwargs['reduce'])).run(conn)
                        overlaps = {k: js_reducer([map_ref[str(v)] for v in raw_overlaps[k]])
                                    for k in raw_overlaps if len(raw_overlaps[k]) > 0}
                    else:
                        overlaps = {k: reduce(kwargs['reduce'], [map_ref[str(v)] for v in raw_overlaps[k]])
                                    for k in raw_overlaps if len(raw_overlaps[k]) > 0}
                else:
                    overlaps = {k: [map_ref[str(v)] for v in raw_overlaps[k]] for k in raw_overlaps if len(raw_overlaps[k]) > 0}
            else:
                overlaps = {uid_ref[k]: {uid_ref[str(v)]: map_ref[str(v)] for v in raw_overlaps[k]} for k in raw_overlaps if len(raw_overlaps[k]) > 0}
        else:
            overlaps = {k: raw_overlaps[k] for k in raw_overlaps if len(raw_overlaps[k]) > 0}
            if uid_ref is not None:
                overlaps = {uid_ref[k]: [uid_ref[str(v)] for v in overlaps[k]] for k in overlaps}

        if 'sort' in kwargs:
            if 'reduce' not in kwargs:
                def get_len(obj):
                    return len(obj[1])
                overlaps = sorted(overlaps.items(), key=get_len, **kwargs['sort'])
            else:
                overlaps = sorted(overlaps.items(), key=operator.itemgetter(1), **kwargs['sort'])
        if 'limit' in kwargs:
            return overlaps[:kwargs['limit']]
        return overlaps


def breadth_first(g_id, node_id, conn, **kwargs):
    if 'topo' in kwargs and kwargs['topo'] in graph_tool_functions:
        return clone_bfs2(g_id, node_id, conn, **kwargs)
    n_id = get_vertex_id(g_id, node_id, conn)
    if n_id is None:
        return {'error': errors['Nonexistence']['node'](g_id, node_id)}
    node_map = {}
    link_map = {}
    discovered_nodes = [[n_id], []]
    discovered_links = [[], []]

    if 'direction' not in kwargs:
        kwargs['direction'] = 'out'
    if 'dist' not in kwargs:
        kwargs['dist'] = 1
    if 'filters' not in kwargs:
        filters = None
    else:
        filters = kwargs['filters']
    lmap = 'lmap' in kwargs
    nmap = 'nmap' in kwargs

    def one_node(n, lvl):
        out = kwargs['direction'] == "out" or (type(kwargs['direction']).__name__ == "list" and kwargs['direction'][lvl-1] == "out")
        if out:
            def do_it(e_id, o, t):
                if t == n_id:
                    return
                if t not in node_map:
                    discovered_links[lvl].append(e_id)
                    discovered_nodes[lvl].append(t)
                    if o in node_map:
                        node_map[t] = node_map[o] + [e_id]
                    else:
                        node_map[t] = [e_id]
            if filters is None or 'link' not in filters[lvl-1]:
                for e in graphs[g_id].vertex(n).out_edges():
                    t = int(e.target())
                    o = int(e.source())
                    do_it(get_edge_id(g_id, e, conn), o, t)
            else:
                l_ids = [get_edge_id(g_id, e, conn) for e in graphs[g_id].vertex(n).out_edges()]
                if len(l_ids) > 0:
                    for o, e, t in auto_reql(r.db(db_id(g_id)).table('links').get_all(*l_ids).filter(filters[lvl-1]['link']).map(
                            lambda l: l['id'].split('_')), conn):
                        o = int(o)
                        t = int(t)
                        do_it("{}_{}_{}".format(o, e, t), o, t)
        else:
            def do_it(e_id, o, t):
                if o == n_id:
                    return
                if o not in node_map:
                    discovered_links[lvl].append(e_id)
                    discovered_nodes[lvl].append(o)
                    if t in node_map:
                        node_map[o] = node_map[t] + [e_id]
                    else:
                        node_map[o] = [e_id]
            if filters is None or 'link' not in filters[lvl-1]:
                for e in graphs[g_id].vertex(n).in_edges():
                    t = int(e.target())
                    o = int(e.source())
                    do_it(get_edge_id(g_id, e, conn), o, t)
            else:
                l_ids = [get_edge_id(g_id, e, conn) for e in graphs[g_id].vertex(n).in_edges()]
                if len(l_ids) > 0:
                    for o, e, t in auto_reql(r.db(db_id(g_id)).table('links').get_all(*l_ids).filter(filters[lvl-1]['link']).map(
                            lambda l: l['id'].split('_')), conn):
                        o = int(o)
                        t = int(t)
                        do_it("{}_{}_{}".format(o, e, t), o, t)

    def nfilt_tier(lvl):
        if filters is not None and 'node' in filters[lvl-1]:
            filtered_node_ids = auto_reql(r.db(db_id(g_id)).table('nodes').get_all(*discovered_nodes[lvl]).filter(
                filters[lvl-1]['node']).map(lambda node: node['id']).coerce_to('array'), conn)
            diff = list(set(discovered_nodes[lvl]) - set(filtered_node_ids))
            for n_id in diff:
                if len(node_map[n_id]) == lvl:
                    del node_map[n_id]
            discovered_nodes[lvl] = [int(v) for v in filtered_node_ids]

    one_node(n_id, 1)
    nfilt_tier(1)
    if kwargs['dist'] > 1:
        for i in range(1, kwargs['dist']):
            if len(discovered_nodes) > i and len(discovered_nodes[i]) > 0:
                discovered_nodes.append([])
                discovered_links.append([])
                map(one_node, discovered_nodes[i], (i + 1 for nd in discovered_nodes[i]))
                nfilt_tier(i+1)
    if nmap:
        if 'js_func' in kwargs:
            n_map = r.js(kwargs['nmap'])
        else:
            def n_map(n):
                return n['id'], kwargs['nmap']
        node_map = {n_val: node_map[n_id] for n_id, n_val in auto_reql(
            r.db(db_id(g_id)).table('nodes').get_all(*node_map.keys()).map(n_map), conn)}

    if lmap:
        if 'js_func' in kwargs:
            l_map = r.js(kwargs['lmap'])
        else:
            def l_map(l):
                return l['id'], kwargs['lmap'](l)
        for tier in discovered_links:
            if len(tier) > 0:
                for l_id, l_val in auto_reql(r.db(db_id(g_id)).table('links').get_all(*tier).map(l_map), conn):
                    link_map[l_id] = l_val
        if 'reduce' in kwargs:
            if 'js_func' in kwargs:
                d = {k: r.expr([link_map[vv] for vv in v]).reduce(r.js(kwargs['reduce'])).run(conn) for k, v in node_map.iteritems()}
            else:
                d = {k: reduce(kwargs['reduce'], [link_map[vv] for vv in v]) for k, v in node_map.iteritems()}
            if 'sort' in kwargs:
                return sorted(d.items(), key=operator.itemgetter(1), **kwargs['sort'])
            return d
        return {k: [link_map[vv] for vv in v] for k, v in node_map.iteritems()}
    return node_map



# Shorthand Collections


node_topo_funcs = {
    'all_links': all_links,
    'out_links': out_links,
    'in_links': in_links,
    'all_neighbors': all_neighbors,
    'in_degree': in_degree,
    'out_degree': out_degree,
    'in_neighbors': in_neighbors,
    'out_neighbors': out_neighbors,
}


link_topo_funcs = {
    'origin': origin,
    'terminus': terminus
}


graph_format = {
    'nodes': add_node,
    'links': add_link,
    'link_types': format_link_type,
    'node_types': format_node_type
}

removers = {
    'nodes': remove_node,
    'links': remove_link,
    'link_types': remove_link_type,
    'node_types': remove_node_type
}

graph_tool_functions = {
    'pagerank': pagerank,
    'betweenness': betweenness,
    'closeness': closeness,
    'eigenvector': eigenvector,
    'katz': katz,
    'hits': hits,
    'hits_hub': hits,
    'hits_authority': hits,
    'central_point_dominance': central_point_dominance,
    'eigentrust': eigentrust,
    'trust_transitivity': trust_transitivity,
    'sfdp': sfdp,
    'fruchterman_reingold': fruchterman_reingold,
    'arf': arf,
    'radial_tree': radial_tree,
    'random_layout': random_layout,
    'shortest_distance': shortest_distance,
    'shortest_path': shortest_path,
    'pseudo_diameter': pseudo_diameter,
    'is_bipartite': is_bipartite,
    'is_planar': is_planar,
    'is_DAG': is_DAG,
    'max_cardinality_matching': max_cardinality_matching,
    'max_independent_node_set': max_independent_node_set,
    'link_reciprocity': link_reciprocity,
    'tsp_tour': tsp_tour,
    'sequential_node_coloring': sequential_node_coloring,
    'similarity': similarity,
    'isomorphism': isomorphism,
    'subgraph_isomorphism': subgraph_isomorphism,
    'min_spanning_tree': min_spanning_tree,
    'random_spanning_tree': random_spanning_tree,
    'dominator_tree': dominator_tree,
    'topological_sort': topological_sort,
    'transitive_closure': transitive_closure,
    'kcore_decomposition': kcore_decomposition
}

graph_generator_functions = {
    'random_graph': random_graph,
    'triangulation': triangulation,
    'lattice': lattice,
    'complete_graph': complete_graph,
    'circular_graph': circular_graph,
    'geometric_graph': geometric_graph,
    'price_network': price_network
}

walkers = {
    'breadth_first': breadth_first,
    'nodes_connected_to': connected_to
}

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
            conn = r.connect()
            if q == "insert":
                total_inserted = 0
                total_replaced = 0
                total_unchanged = 0
                total_errors = 0
                failures = []
                batch = []
                params = json.loads(head['params'])
                if 'conflict' not in params:
                    params['conflict'] = "error"
                elif params['type'] not in acceptable_types:
                    json.dumps({'error': errors['SyntaxError']['graph'](g_id, params['type'])})
                if 'durability' in params:
                    dur = params['durability']
                else:
                    dur = 'hard'

                def process_batch():
                    d = auto_reql(r.db(dbid).table(params['type']).insert(
                        batch, conflict=params['conflict'], durability=dur), conn)
                    del batch[:]
                    return d

                if 'return_failures' in params:
                    r_fail = params['return_failures']
                else:
                    r_fail = True
                for doc in tab_separate(cherrypy.request.body, delim=self.delim):
                    doc = json.loads(doc)
                    try:
                        to_add = graph_format[params['type']](g_id, doc, params['conflict'], conn)
                        batch.append(to_add)
                    except (ValueError, TypeError), m:
                        if r_fail:
                            failures.append({params['type'][:-1]: doc, 'error': unicode(m)})
                    if len(batch) >= self.chunk_limit:
                        d = process_batch()
                        total_inserted += d['inserted']
                        if 'replaced' in d:
                            total_replaced += d['replaced']
                        if 'unchanged' in d:
                            total_unchanged += d['unchanged']
                        if 'errors' in d:
                            total_errors += d['errors']
                if len(batch) > 0:
                    d = process_batch()
                    total_inserted += d['inserted']
                    if 'replaced' in d:
                        total_replaced += d['replaced']
                    if 'unchanged' in d:
                        total_unchanged += d['unchanged']
                    if 'errors' in d:
                        total_errors += d['errors']
                answer = {'inserted': total_inserted, 'replaced': total_replaced, 'unchanged': total_unchanged,
                          'errors': total_errors}
                if r_fail:
                    answer['failures'] = failures

                return json.dumps(answer)
            elif q == "create_graph":
                return json.dumps(create_graph(g_id, c=conn))
            elif q == "drop_graph":
                num_nodes = graphs[g_id].num_vertices()
                num_links = graphs[g_id].num_edges()
                try:
                    auto_reql(r.db_drop(g_id), conn)
                except r.ReqlOpFailedError:
                    pass
                purge_graph(g_id)
                return json.dumps({'graphs_dropped': 1, 'nodes_deleted': num_nodes, 'links_deleted': num_links})
            elif q == "pluck":
                params, body, ot, js_func, event_stream = p_b_ot()
                obj_id = params['obj_id']
                obj_id, uid, id_quote = id_or_uid(obj_id, ot)
                if 'nested' in body:
                    nested = body['nested']
                else:
                    nested = []
                msg = errors['Nonexistence'][ot](g_id, obj_id)
                if ot+'s' in acceptable_types:
                    qu = r.db(dbid).table(ot + 's')
                    if not uid:
                        qu = qu.get(obj_id)
                    else:
                        qu = qu.get_all(obj_id, index='uid')
                    for nest in nested:
                        qu = qu[trim_id(nest)]
                    if uid:
                        qu = qu.coerce_to('array')
                    try:
                        d = auto_reql(qu, conn)
                    except r.ReqlNonExistenceError:
                        return json.dumps({'error': msg})
                    if uid:
                        if len(d) == 0:
                            return json.dumps({'error': msg})
                        else:
                            d = d[0]
                    return json.dumps(d)
                elif ot == "property_map":
                    if g_id in property_maps and obj_id in property_maps[g_id]:
                        pm = property_maps[g_id][obj_id]
                        if pm.key_type() == "v":
                            s_id = get_vertex_id(g_id, body['select'], conn)
                            if s_id is None:
                                return json.dumps({'error': errors['Nonexistence']['node'](g_id, body['select'])})
                        elif pm.key_type() == "e":
                            s_id = get_edge_id(g_id, body['select'], conn)
                            if s_id is None:
                                return json.dumps({'error': errors['Nonexistence']['link'](g_id, body['select'])})
                            o, eid, t = [int(v) for v in s_id.split('_')]
                            s_id = get_edge(graphs[g_id], o, t, eid)
                        else:
                            return json.dumps({'error': 'An unknown error occurred during property map retrieval.'})
                        return json.dumps(pm[s_id])
                    else:
                        return json.dumps({'error': msg})
            elif q == "stream":
                params, body, ot, js_func, event_stream = p_b_ot()
                coerce_to = 'stream'
                stream = []
                if 'count' in body and body['count']:
                    count = True
                else:
                    count = False
                if ot in acceptable_types:
                    qu = r.db(dbid).table(ot)
                    whole_table = True
                    if 'get_all' in body:
                        if 'index' in body:
                            qu = qu.get_all(*body['get_all'], index=body['index'])
                        else:
                            qu = qu.get_all(*body['get_all'])
                        whole_table = False
                    if 'filter' in body:
                        filt_func = body['filter']
                        if js_func:
                            filt_func = r.js(filt_func)
                        qu = qu.filter(filt_func)
                        whole_table = False
                    if 'map' in body:
                        map_func = body['map']
                        if js_func:
                            map_func = r.js(map_func)
                        qu = qu.map(map_func)
                        whole_table = False
                    if 'reduce' in body:
                        reduce_func = body['reduce']
                        if js_func:
                            reduce_func = r.js(reduce_func)
                        qu = qu.reduce(reduce_func)
                        whole_table = False
                    else:
                        if 'map' in body:
                            if 'coerce_to' in body and body['coerce_to'] == 'property_map':
                                if ot == 'nodes':
                                    return json.dumps(
                                        node_property_map(g_id, body['pmap_name'], body['pmap_type'], map_func, conn))
                                elif ot == 'links':
                                    return json.dumps(
                                        link_property_map(g_id, body['pmap_name'], body['pmap_type'], map_func, conn))
                    if 'nested' in body:
                        nested = body['nested']
                        whole_table = False
                    else:
                        nested = []
                    for nest in nested:
                        qu = qu[trim_id(nest)]
                    if 'sort' in body:
                        if 'order_by' not in body['sort']:
                            return json.dumps({
                                'error': {
                                    'msg': "sort() requires the 'order_by' parameter in this context.",
                                    'type': "PreqlSyntaxError"
                                }})
                        else:
                            ob = body['sort']['order_by']
                        if js_func and not isinstance(ob, dict):
                            ob = r.js(ob)
                        if 'reverse' in body['sort'] and body['sort']['reverse']:
                            ob = r.desc(ob)
                        qu = qu.order_by(ob)
                    if 'limit' in q:
                        qu = qu.limit(q['limit'])
                    if count:
                        if whole_table:
                            if ot == "nodes":
                                return json.dumps(graphs[g_id].num_vertices())
                            elif ot == "links":
                                return json.dumps(graphs[g_id].num_edges())
                        return json.dumps(auto_reql(qu.count(), conn))
                    if 'coerce_to' in body:
                        qu = qu.coerce_to(body['coerce_to'])
                    stream = auto_reql(qu, conn)

                elif ot == 'property_map':
                    if 'property_map' not in body:
                        return json.dumps({'error': errors['MissingFields']['id'][q['type']](g_id)}) + self.delim
                    if g_id not in property_maps or body['property_map'] not in property_maps[g_id]:
                        return json.dumps(
                            {'error': errors['Nonexistence'][ot](g_id, body['property_map'])}) + self.delim
                    uids = 'uids' in body and body['uids']

                    pm = property_maps[g_id][body['property_map']]
                    key_type = pm.key_type()

                    def pm_stream():
                        g = graphs[g_id]
                        if 'sort' in body:
                            if 'vector' in pm.value_type():
                                yield json.dumps({'error': errors['property_map_sort'](
                                    g_id, body['property_map'], pm.value_type(), body['sort'])})+self.delim
                                raise StopIteration
                            if 'kind' in body['sort']:
                                kind = body['sort']['kind']
                            else:
                                kind = "quicksort"
                            ind_order = numpy.argsort(pm.get_array(), kind=kind)
                            if 'reverse' in body['sort'] and body['sort']['reverse']:
                                ind_order = ind_order[::-1]
                        if key_type == "v":
                            qu = r.db(dbid).table('nodes')
                            if 'get_all' in body:
                                ga_params = {}
                                if 'index' in body:
                                    ga_params['index'] = body['index']
                                qu = qu.get_all(*body['get_all'], **ga_params)
                            if 'filter' in body:
                                filt_func = body['filter']
                                if js_func:
                                    filt_func = r.js(filt_func)
                                qu = qu.filter(filt_func)
                            if uids:
                                qu = qu.map(lambda node: (node['id'], node['uid']))
                                if 'sort' in body:
                                    tripples = [[n_id, uid, numpy.where(ind_order == n_id)[0][0]] for n_id, uid in auto_reql(qu, conn)]
                                    pull_order = sorted(tripples, key=operator.itemgetter(2))
                                    if 'limit' in body:
                                        pull_order = pull_order[:body['limit']]
                                    pull_order = [(x, y) for x, y, z in pull_order]
                                else:
                                    if 'limit' in body:
                                        qu = qu.limit(body['limit'])
                                    pull_order = auto_reql(qu, conn)
                                for n_id, uid in pull_order:
                                    if 'vector' in type(pm[n_id]).__name__:
                                        vd = list(pm[n_id])
                                    else:
                                        vd = pm[n_id]
                                    yield [uid, vd]
                            else:
                                if 'get_all' in body or 'filter' in body:
                                    qu = qu['id']
                                    if 'sort' in body:
                                        doubles = [[n_id, numpy.where(ind_order == n_id)[0][0]] for n_id in auto_reql(qu, conn)]
                                        selection = sorted(doubles, key=operator.itemgetter(1))
                                        if 'limit' in body:
                                            selection = selection[:body['limit']]
                                        selection = [x for x, y in selection]
                                    else:
                                        if 'limit' in body:
                                            qu = qu.limit(body['limit'])
                                        selection = auto_reql(qu, conn)
                                else:
                                    if 'sort' in body:
                                        selection = ind_order
                                    else:
                                        selection = g.vertices()
                                    if 'limit' in body:
                                        selection = itertools.islice(selection, body['limit'])
                                for v in selection:
                                    if 'Vector' in type(pm[v]).__name__:
                                        vd = list(pm[v])
                                    else:
                                        vd = pm[v]
                                    yield [int(v), vd]
                        elif key_type == "e":
                            qu = r.db(dbid).table('links')
                            if 'get_all' in body:
                                ga_params = {}
                                if 'index' in body:
                                    ga_params['index'] = body['index']
                                qu = qu.get_all(*body['get_all'], **ga_params)
                            if 'filter' in body:
                                filt_func = body['filter']
                                if js_func:
                                    filt_func = r.js(filt_func)
                                qu = qu.filter(filt_func)
                            if uids:
                                qu = qu.map(lambda link: (link['id'].split('_'), link['uid']))
                                if 'sort' in body:
                                    def edge_tripples(l_id, uid):
                                        o, eid, t = [int(v) for v in l_id]
                                        e = get_edge(g, o, t, eid)
                                        return [e, uid, numpy.where(ind_order == g.edge_index[e])[0][0]]
                                    tripples = [edge_tripples(l_id, uid) for l_id, uid in auto_reql(qu, conn)]
                                    pull_order = sorted(tripples, key=operator.itemgetter(2))
                                    if 'limit' in body:
                                        pull_order = pull_order[:body['limit']]
                                    pull_order = [(x, y) for x, y, z in pull_order]
                                    for e, uid in pull_order:
                                        yield [uid, pm[e]]
                                else:
                                    if 'limit' in body:
                                        qu = qu.limit(body['limit'])
                                    for l_id, uid in auto_reql(qu, conn):
                                        es = g.edge(l_id[0], l_id[2], all_edges=True)
                                        for e in es:
                                            if g.edge_properties['id'][e] == int(l_id[1]):
                                                if 'Vector' in type(pm[e]).__name__:
                                                    ed = list(pm[e])
                                                else:
                                                    ed = pm[e]
                                                yield [uid, ed]
                            else:
                                if 'get_all' in body or 'filter' in body:
                                    qu = qu['id'].map(lambda val: val.split('_'))
                                    if 'sort' in body:
                                        def get_doubles(o, eid, t):
                                            e = get_edge(g_id, int(o), int(t), int(eid))
                                            return [e, numpy.where(ind_order == g.edge_index[e])[0][0]]
                                        doubles = [get_doubles(o, eid, t) for [o, eid, t] in auto_reql(qu, conn)]
                                        selection = sorted(doubles, key=operator.itemgetter(1))
                                        if 'limit' in body:
                                            selection = selection[:body['limit']]
                                        selection = [x for [x, y] in selection]
                                    else:
                                        selection = [get_edge(g, int(o), int(t), int(eid)) for [o, eid, t] in auto_reql(qu, conn)]
                                else:
                                    if 'sort' in body:
                                        id_map = numpy.empty(g.num_edges(), dtype='object')
                                        for e in g.edges():
                                            id_map[g.edge_index[e]] = e
                                        selection = [id_map[ind] for ind in ind_order]
                                    else:
                                        selection = g.edges()
                                    if 'limit' in body:
                                        selection = itertools.islice(selection, body['limit'])
                                for e in selection:
                                    if 'Vector' in type(pm[e]).__name__:
                                        ed = list(pm[e])
                                    else:
                                        ed = pm[e]
                                    yield [get_edge_id(g_id, e, conn), ed]

                    stream = pm_stream()

                    if 'coerce_to' in body:
                        coerce_to = body['coerce_to']

                elif ot == "property_maps":
                    prep_pm(g_id)

                    def pms():
                        k_types = {
                            'v': 'node',
                            'e': 'link'
                        }
                        for k in property_maps[g_id]:
                            try:
                                yield {
                                    'id': k,
                                    'key_type': k_types[property_maps[g_id][k].key_type()],
                                    'value_type': property_maps[g_id][k].value_type()
                                }
                            except AttributeError:
                                print k,
                                print property_maps[g_id][k]
                    stream = pms()
                elif ot == "array":
                    if 'array' not in body:
                        return json.dumps({'error': errors['MissingFields']['id'][ot](g_id)}) + self.delim
                    if g_id not in ndarrays or body['array'] not in ndarrays[g_id]:
                        return json.dumps(
                            {'error': errors['Nonexistence'][ot](g_id, body['array'])}) + self.delim
                    stream = ndarrays[g_id][body['array']]
                elif ot == "arrays":
                    prep_pm(g_id)

                    def arrays_stream():
                        for k in ndarrays[g_id]:
                            yield {'id': k, 'value_type': str(ndarrays[g_id][k].dtype)}

                    stream = arrays_stream()
                if 'coerce_to' in body:
                    coerce_to = body['coerce_to']
                return stream_gen(stream, event_stream, coerce_to, count)
            elif q == "update":
                params, body, obj_type, js_func, event_stream = p_b_ot()
                if obj_type + 's' in acceptable_types:
                    obj_id, uid, id_quote = id_or_uid(obj_type, params['obj_id'])
                    update = body['update']
                    msg = errors['Nonexistence'][obj_type](g_id, obj_id)
                    qu = r.db(dbid).table(obj_type + 's')
                    if not uid:
                        try:
                            dd = auto_reql(qu.get(obj_id).update(update), conn)
                        except r.ReqlNonExistenceError:
                            return json.dumps({'error': msg})
                    else:
                        dd = auto_reql(qu.get_all(obj_id, index='uid').update(update), conn)
                    return json.dumps(dd)
                elif obj_type in acceptable_types:
                    update = body['update']

                    def literalize(d):
                        for k, v in d.iteritems():
                            if isinstance(v, dict):
                                literalize(v)
                            else:
                                m = literal_check.search(v)
                                if m:
                                    d[k] = r.literal(json.loads(m.group('json_doc')))
                    literalize(update)
                    qu = r.db(dbid).table(obj_type)
                    if 'get_all' in body:
                        if 'index' in body:
                            qu = qu.get_all(*body['get_all'], index=body['index'])
                        else:
                            qu = qu.get_all(*body['get_all'])
                    if 'filter' in body:
                        filt_func = body['filter']
                        if js_func:
                            filt_func = r.js(filt_func)
                        qu = qu.filter(filt_func)
                    d = auto_reql(qu.update(update), conn)
                    return json.dumps(d)
            elif q == "delete":
                params, body, obj_type, js_func, event_stream = p_b_ot()
                qu = r.db(dbid)
                resp = {
                    'nodes_deleted': 0,
                    'links_deleted': [],
                    'nodes_updated': {},
                    'links_updated': [],
                    'node_types_deleted': 0,
                    'link_types_deleted': 0
                }
                if obj_type in acceptable_types:
                    qu = qu.table(obj_type)
                    if 'get_all' in body:
                        if 'index' in body:
                            qu = qu.get_all(*body['get_all'], index=body['index'])
                        else:
                            qu = qu.get_all(*body['get_all'])
                    if 'filter' in body:
                        filt_func = body['filter']
                        if js_func:
                            filt_func = r.js(filt_func)
                        qu = qu.filter(filt_func)
                    if 'get_all' not in body and 'filter' not in body:
                        deleted = auto_reql(qu.delete(), conn)['deleted']
                        resp = {obj_type + "_deleted": deleted}
                        if obj_type == "links":
                            graphs[g_id].clear_edges()
                        elif obj_type == "nodes":
                            graphs[g_id].clear()
                            resp['links_deleted'] = auto_reql(r.db(dbid).table('links').delete(), conn)['deleted']
                        elif obj_type == "node_types":
                            resp['nodes_updated'] = auto_reql(r.db(dbid).table('nodes').update(
                                {'type': 'Node'}), conn)['replaced']
                        elif obj_type == "link_types":
                            resp['links_updated'] = auto_reql(r.db(dbid).table('links').update(
                                {'type': 'Link'}), conn)['replaced']
                        return json.dumps(resp)
                elif obj_type+'s' in acceptable_types:
                    obj_id, uid, id_quote = id_or_uid(params['obj_id'], obj_type)
                    msg = errors['Nonexistence'][obj_type](g_id, obj_id)
                    obj_type += 's'
                    if uid:
                        d = auto_reql(
                            r.db(dbid).table(obj_type).get_all(obj_id, index='uid')['id'].coerce_to('array'), conn)
                        if len(d) > 0:
                            obj_id = d[0]
                        else:
                            return json.dumps({'error': msg})
                    else:
                        return json.dumps(
                            {'error': errors['SyntaxError']['uid_required'](g_id, obj_type, obj_id, 'delete')})
                    return json.dumps(removers[obj_type](g_id, obj_id, conn))
                elif obj_type == "property_map":
                    obj_id = params['obj_id']
                    if obj_id in property_maps[g_id]:
                        del property_maps[g_id][obj_id]
                        return json.dumps({'deleted': obj_id})
                elif obj_type == "array":
                    obj_id = params['obj_id']
                    if obj_id in ndarrays[g_id]:
                        del ndarrays[g_id][obj_id]
                        return json.dumps({'deleted': obj_id})
                elif obj_type == "property_maps":
                    return
                elif obj_type == "arrays":
                    return
                if '_type' in obj_type:
                    qu = qu['id']
                else:
                    qu = qu['uid']

                for o_id in auto_reql(qu, conn):
                    if obj_type == "nodes":
                        o_id = get_vertex_id(g_id, o_id, conn)
                    d = removers[obj_type](g_id, o_id, conn)
                    if obj_type in ['nodes', 'links']:
                        resp['links_deleted'] += d['links_deleted']
                        resp['links_updated'] += d['links_updated']
                    if obj_type == "nodes":
                        resp['nodes_deleted'] += d['nodes_deleted']
                        resp['nodes_updated'].update(d['nodes_updated'])
                    elif obj_type == "node_types":
                        resp['node_types_deleted'] += d['node_types_deleted']
                        resp['nodes_updated'].update(d['nodes_updated'])
                    elif obj_type == "link_types":
                        resp['link_types_deleted'] += d['link_types_deleted']
                        resp['links_updated'] += d['links_updated']
                return json.dumps(resp)
            elif q == "topology":
                params, body, obj_type, js_func, event_stream = p_b_ot()

                if obj_type == 'node' and body['topo'] in node_topo_funcs:
                    if js_func:
                        if 'filter' in body['topo_params'] and not isinstance(body['topo_params']['filter'], dict):
                            body['topo_params']['filter'] = r.js(body['topo_params']['filter'])
                        if 'nodes' in body['topo_params'] and not isinstance(body['topo_params']['nodes'], dict):
                            body['topo_params']['nodes'] = r.js(body['topo_params']['nodes'])
                        if 'links' in body['topo_params'] and not isinstance(body['topo_params']['links'], dict):
                            body['topo_params']['links'] = r.js(body['topo_params']['links'])
                    return json.dumps(node_topo_funcs[body['topo']](g_id, params['obj_id'], conn, **body['topo_params']))
                elif obj_type == 'link' and body['topo'] in link_topo_funcs:
                    return json.dumps(link_topo_funcs[body['topo']](g_id, params['obj_id'], conn, **body['topo_params']))
                elif body['topo'] in graph_tool_functions:
                    return json.dumps(graph_tool_functions[body['topo']](g_id, conn, **body['topo_params']))
                return json.dumps({'error': errors['SyntaxError']['graph'](g_id, obj_type)})
            elif q == "generate":
                params, body, obj_type, js_func, event_stream = p_b_ot()
                if body['gen_type'] not in graph_generator_functions:
                    return json.dumps({'error': errors['SyntaxError']['graph'](g_id, body['gen_type'])})
                return json.dumps(graph_generator_functions[body['gen_type']](g_id, conn, **body['gen_params']))
            elif q == "commit":
                g = prep_pm(g_id)
                params, body, obj_type, js_func, event_stream = p_b_ot()

                if body['property_map'] in property_maps[g_id]:
                    fp = get_field_list(body['commit_target'])
                    pm = property_maps[g_id][body['property_map']]
                    key_type = pm.key_type()
                    val_type = pm.value_type()
                    response = {'replaced': 0, 'unchanged': 0, 'skipped': 0, 'errors': 0}

                    def increment_response(rdb_response):
                        response['replaced'] += rdb_response['replaced']
                        response['unchanged'] += rdb_response['unchanged']
                        response['skipped'] += rdb_response['skipped']
                        response['errors'] += rdb_response['errors']

                    if key_type == "v":
                        for v in g.vertices():
                            val = pm[v]
                            try:
                                if 'vector' in val_type:
                                    val = [invalid_float_replacer(x) for x in val]
                                elif val_type == 'bool':
                                    val = bool(val)
                                else:
                                    val = invalid_float_replacer(val)
                                d = auto_reql(r.db(dbid).table('nodes').get(int(v)).update(
                                    update_formatter(fp, val)), conn)
                                increment_response(d)
                            except (ValueError, TypeError) as err:
                                print err
                                print val
                                print type(val).__name__
                    elif key_type == 'e':
                        for e in g.edges():
                            val = pm[e]
                            try:
                                if 'Vector' in type(val).__name__:
                                    val = [invalid_float_replacer(x) for x in list(val)]
                                else:
                                    val = invalid_float_replacer(val)
                                d = auto_reql(
                                    r.db(dbid).table('links').get(get_edge_id(dbid, e, conn)).update(
                                        update_formatter(fp, val)), conn)
                                increment_response(d)
                            except ValueError:
                                print val
                                print type(val).__name__
                    return json.dumps(response)
            elif q == "graph_filter":
                body = pickle.loads(cherrypy.request.body.read())
                filters = body['filter']
                if 'nodes' in filters and filters['nodes'] is not None:
                    nf_name = str(uuid4())
                    nfn = node_property_map(g_id, nf_name, 'bool', filters['nodes'], conn)['property_map']
                    nf = property_maps[g_id][nfn]
                else:
                    nf = None
                if 'links' in filters and filters['links'] is not None:
                    lf_name = str(uuid4())
                    lfn = link_property_map(g_id, lf_name, 'bool', filters['links'], conn)['property_map']
                    lf = property_maps[g_id][lfn]
                else:
                    lf = None
                if 'directed' in filters and filters['directed'] is not None:
                    directed = filters['directed']
                else:
                    directed = None
                if 'reversed' in filters and filters['reversed'] is not None:
                    rev = filters['reversed']
                else:
                    rev = False
                if 'filter_id' in filters and filters['filter_id'] is not None:
                    g2_id = filters['filter_id']
                else:
                    g2_id = str(uuid4()).replace('-', '_')
                g2 = gt.GraphView(graphs[g_id], vfilt=nf, efilt=lf, directed=directed, reversed=rev)
                graphs[g2_id] = g2
                prep_pm(g2_id)
                for pm in property_maps[g_id]:
                    property_maps[g2_id][pm] = property_maps[g_id][pm]
                for nda in ndarrays[g_id]:
                    ndarrays[g2_id][nda] = ndarrays[g_id][nda]
                return json.dumps({'subgraph': g2_id})
            elif q == "walk":
                params, body, obj_type, js_func, event_stream = p_b_ot()

                if 'coerce_to' in body:
                    coerce_to = body['coerce_to']
                else:
                    coerce_to = "stream"
                if 'count' in body and body['count']:
                    count = True
                else:
                    count = False
                if body['alg'] in walkers:
                    if js_func:
                        if 'filters' in body['walk_rules']:
                            for filt in body['walk_rules']['filters']:
                                if 'link' in filt and not isinstance(filt['link'], dict):
                                    filt['link'] = r.js(filt['link'])
                                if 'node' in filt and not isinstance(filt['node'], dict):
                                    filt['node'] = r.js(filt['node'])
                        body['walk_rules']['js_func'] = True
                    resp = walkers[body['alg']](g_id, params['obj_id'], conn, **body['walk_rules'])
                    if type(resp).__name__ == 'dict':
                        if count:
                            return json.dumps(len(resp))
                        return json.dumps(resp)
                    else:
                        return stream_gen(resp, event_stream, coerce_to, count)
            elif q == "fields":
                obj_type = json.loads(head['params'])['type']
                if obj_type in acceptable_types:
                    fields = auto_reql(r.db(dbid).table(obj_type).map(lambda n: n.keys()).reduce(
                        lambda x, y: r.expr(x + y).distinct()), conn)
                    return json.dumps(fields)
            elif q == "create_index":
                params, body, obj_type, js_func, event_stream = p_b_ot()
                dbid = db_id(g_id)
                if obj_type in acceptable_types:
                    if js_func and 'definition' in body and body['definition'] is not None:
                        body['definition'] = r.js(body['definition'])
                    index_params = [v for v in [body['index_name'], body['definition']] if v is not None]
                    try:
                        auto_reql(r.db(dbid).table(obj_type).index_create(*index_params), conn)
                    except r.ReqlOpFailedError:
                        return json.dumps({'error': errors['IDDuplicates']['index'](g_id, obj_type,
                                                                                    body['index_name'],
                                                                                    body['definition'])})
                    resp = auto_reql(r.db(dbid).table(obj_type).index_wait(), conn)
                    return json.dumps([d['index'] for d in resp])
            elif q == "graph_stats":
                link_types = auto_reql(
                    r.db(dbid).table('link_types').map(lambda lt: lt['id']).coerce_to('array'), conn)
                node_types = auto_reql(
                    r.db(dbid).table('node_types').map(lambda lt: lt['id']).coerce_to('array'), conn)
                summary = {
                    'id': g_id,
                    'num_nodes': graphs[g_id].num_vertices(),
                    'num_links': graphs[g_id].num_edges(),
                    'link_types': link_types,
                    'node_types': node_types,
                }
                if type(graphs[g_id]).__name__ == "GraphView":
                    summary['filtered_from'] = graphs[g_id].base.graph_properties['id']
                return json.dumps(summary)
        else:
            wrong_key()
    q.exposed = True
    q._cp_config = {'response.stream': True}


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
    dbs = auto_reql(r.db_list(), r_conn)
    for n in dbs:
        if n != "test" and n != "rethinkdb":
            temp_g = load_graph(n, r_conn)
            graphs[n] = temp_g

    cherrypy.tree.mount(API(), '/preql')
    cherrypy.engine.timeout_monitor.unsubscribe()
    cherrypy.engine.start()
    cherrypy.engine.block()