def quote_type(val):
    if type(val).__name__ in ['str', 'unicode']:
        return "'"
    else:
        return ''


def quoted_val(val):
    qt = quote_type(val)
    qted = "{}{}{}".format(qt, val, qt)
    return qted


def qt_cap(offending_value, doc_type):
    qted = quoted_val(offending_value)
    if "_" not in doc_type:
        cap_doc = doc_type.capitalize()
    else:
        cap_doc = "{}{}".format(*[b.capitalize() for b in doc_type.split('_')])
    return qted, cap_doc


def error_format(error_type, query, offending_value, explanation):
    q_split = query.split(str(offending_value))
    id_space = ''.join([' ' for i in range(len(q_split[0]))])
    id_underscore = ''.join(['^' for i in range(len(str(offending_value)))])
    error_underline = "".join(['-' for i in range(min(len(explanation), 50))])
    msg = "\n{}\n" \
          "{}\n" \
          "{}{}{}\n" \
          "{}{}\n" \
          "{}\n" \
          "{}\n".format("SynthDB.{}".format(error_type), error_underline, q_split[0], offending_value, q_split[1], id_space,
                        id_underscore, error_underline, explanation)
    return msg


def doc_nonexistence_maker(doc_type):
    def f(g_id, offending_value):
        qted, cap_doc = qt_cap(offending_value, doc_type)
        query = "SynthDB.graph('{}').{}({})...".format(g_id, doc_type, qted)
        expl = "Graph('{}') does not a contain {}({}).".format(g_id, cap_doc, qted)
        err_type = "NonexistenceError"
        return {'type': err_type, 'msg': error_format(err_type, query, offending_value, expl)}
    return f


def protected_doc_maker(doc_type):
    def f(g_id, offending_value):
        qted, cap_doc = qt_cap(offending_value, doc_type)
        query = "SynthDB.graph('{}').{}({}).delete()".format(g_id, doc_type, qted)
        expl = "{} is a protected {} and cannot be deleted.".format(qted, cap_doc)
        err_type = "InvalidOperationError"
        return {'type': err_type, 'msg': error_format(err_type, query, offending_value, expl)}
    return f


def rethinkDBNonexistence():
    line1 = "Could not connect to RethinkDB at localhost:28015."
    line2 = "Please check that RethinkDB is running, and has access permissions to the drive it is configured to use."
    line3 = "Run 'sudo nano /etc/rethinkdb/instances.d/SynthDB.conf' to configure RethinkDB as described here:"
    line4 = "https://rethinkdb.com/docs/config-file/"
    line5 = "Then run `sudo /etc/init.d/rethinkdb restart` to start the RethinkDB server."
    id_underscore = ''.join(['-' for i in range(len(line5))])
    msg = "{}\n" \
          "{}\n" \
          "{}\n\n" \
          "{}\n\n" \
          "{}\n\n" \
          "{}\n" \
          "{}".format(id_underscore, line1, line2, line3, line4, line5, id_underscore)
    return msg


def graphNonexistence(offending_value):
    expl = "A graph with ID '{}' could not be found.".format(offending_value)
    query = "SynthDB.graph('{}')...".format(offending_value)
    err_type = "NonexistenceError"
    return {'type': err_type, 'msg': error_format(err_type, query, offending_value, expl)}


def graphIDInUse(g_id):
    query = "SynthDB.create_graph('{}')...".format(g_id)
    expl = "Graph('{}') already exists. You must use a unique name for a new Graph.".format(g_id)
    err_type = "DuplicateIDError"
    return {'type': err_type, 'msg': error_format(err_type, query, g_id, expl)}


def indexIDInUse(g_id, doc_type, wrong_index, definition=None):
    if definition is None:
        index_params = "'{}'".format(wrong_index)
    else:
        index_params = "'{}', {}".format(wrong_index, definition)
    query = "SynthDB.graph('{}').{}().create_index({})".format(g_id, doc_type, index_params)
    expl = "Graph('{}') already has a {} index by the name of '{}'".format(g_id, doc_type[:-1], wrong_index)
    err_type = "DuplicateIDError"
    return {'type': err_type, 'msg': error_format(err_type, query, wrong_index, expl)}


def graphIDmising():
    offending = "?????"
    query = "SynthDB.graph({})".format(offending)
    expl = "You must specify a Graph ID to make a query."
    err_type = "InvalidOperationError"
    return {'type': err_type, 'msg': error_format(err_type, query, offending, expl)}


def missing_id_maker(doc_type):
    def f(g_id):
        offending = "?????"
        qted, cap_doc = qt_cap(offending, doc_type)
        query = "SynthDB.graph('{}').{}({})".format(g_id, doc_type, offending)
        expl = "You must specify a {} ID (or UID)".format(cap_doc)
        err_type = "InvalidOperationError"
        return {'type': err_type, 'msg': error_format(err_type, query, offending, expl)}
    return f


def invalid_graph_query(g_id, offending_value):
    query = "SynthDB.graph('{}').{}()".format(g_id, offending_value)
    expl = "'{}()' cannot be called on a Graph.".format(offending_value)
    err_type = "PreqlSyntaxError"
    return {'type': err_type, 'msg': error_format(err_type, query, offending_value, expl)}


def invalid_subgraph_query(g_id, n_id, walker, walk_params, topo, topo_params, extra=''):
    offending_value = "{}({})".format(topo, param_stringer(topo_params))
    q = "SynthDB.graph('{}').node({}).{}({}).{}".format(g_id, quoted_val(n_id), walker, param_stringer(walk_params), offending_value)
    expl = "'{}' is not a valid query for induced subgraphs. {}".format(offending_value, extra)
    err_type = "PreqlSyntaxError"
    return {'type': err_type, 'msg': error_format(err_type, q, offending_value, expl)}


def invalid_table_query_maker(doc_type):
    def f(g_id, offending_value):
        qted, cap_doc = qt_cap(offending_value, doc_type)
        query = "SynthDB.graph('{}').{}().{}()".format(g_id, doc_type, offending_value)
        expl = "'{}()' cannot be called on the {} stream.".format(offending_value, cap_doc)
        err_type = "PreqlSyntaxError"
        return {'type': err_type, 'msg': error_format(err_type, query, offending_value, expl)}
    return f


def invalid_doc_query_maker(doc_type):
    def f(g_id, obj_id, offending_value):
        qted, cap_doc = qt_cap(obj_id, doc_type)
        query = "SynthDB.graph('{}').{}({}).{}()".format(g_id, doc_type, qted, offending_value)
        expl = "'{}()' cannot be called on a {}".format(offending_value, cap_doc)
        err_type = "PreqlSyntaxError"
        return {'type': err_type, 'msg': error_format(err_type, query, offending_value, expl)}
    return f


def param_stringer(params):
    kwargs = ''.join(["{}={}, ".format(k, quoted_val(params[k])) for k in params])
    kwargs = kwargs[:-2]
    if kwargs == '':
        kwargs = "..."
    return kwargs


def missing_fields_graph_topo(g_id, func, params, missing, generator=False):
    kwargs = param_stringer(params)
    if generator:
        prefix = "create_"
    else:
        prefix = ''
    query = "SynthDB.{}graph('{}').{}({})".format(prefix, g_id, func, kwargs)
    miss_txt = ''.join(['{}=, '.format(mv) for mv in missing])
    miss_txt = miss_txt[:-2]
    expl = "'{}' requires the following missing fields: {}".format(func, miss_txt)
    err_type = "PreqlSyntaxError"
    return {'type': err_type, 'msg': error_format(err_type, query, kwargs, expl)}


def wrong_format_graph_topo(g_id, func, params, type_error, generator=False):
    kwargs = param_stringer(params)
    if generator:
        prefix = "create_"
    else:
        prefix = ''
    query = "SynthDB.{}graph('{}').{}({})".format(prefix, g_id, func, kwargs)
    expl = "The '{}' parameter requires a {} value.".format(type_error['key'], type_error['correct_type'])
    err_type = "PreqlSyntaxError"
    return {'type': err_type, 'msg': error_format(err_type, query, type_error['value'], expl)}


def needs_topo(g_id, topo_query, req):
    query = "SynthDB.graph('{}').{}()".format(g_id, topo_query)
    expl = "{}() requires {}".format(topo_query, req)
    err_type = "TopologyError"
    return {'type': err_type, 'msg': error_format(err_type, query, g_id, expl)}


def pm_sort_error(g_id, pm_id, value_type, sort_params):
    query = "SynthDB.graph('{}').property_map('{}').sort({})".format(g_id, pm_id, param_stringer(sort_params))
    expl = "PropertyMap('{}') contains {} values, which cannot be sorted by this function.".format(pm_id, value_type)
    err_type = "ValueTypeError"
    return {'type': err_type, 'msg': error_format(err_type, query, pm_id, expl)}


def uid_required(g_id, doc_type, doc_id, oper, query_params=None):
    wrong_val = quoted_val(doc_id)
    if query_params is not None:
        params = param_stringer(query_params)
    else:
        params = ''
    query = "SynthDB.graph('{}').{}({}).{}({})".format(g_id, doc_type, wrong_val, oper, params)
    expl = "{}({}).{}({}) requires a 'uid' reference, and cannot be completed with an 'id'.".format(doc_type, wrong_val, oper, params)
    err_type = "PreqlSyntaxError"
    return {'type': err_type, 'msg': error_format(err_type, query, wrong_val, expl)}


def limits_exceeded(g_id, doc_type, limit):
    query = "SynthDB.graph('{}').insert_{}s(...)".format(g_id, doc_type)
    expl = "graph('{}') has already met it's limit of {} {}s".format(g_id, limit, doc_type)
    err_type = "LimitsExceededError"
    return {'type': err_type, 'msg': error_format(err_type, query, '...', expl)}


doc_types = ['node', 'link', 'node_type', 'link_type', 'property_map', 'array']

nonexistence = {'graph': graphNonexistence}
missing_ids = {'graph': graphIDmising}
protected = {}
invalid_queries = {
    'graph': invalid_graph_query,
    'graph_topo': wrong_format_graph_topo,
    'subgraph': invalid_subgraph_query,
    'uid_required': uid_required
}

for dt in doc_types:
    tbl = dt+'s'
    nonexistence[dt] = doc_nonexistence_maker(dt)
    missing_ids[dt] = missing_id_maker(dt)
    protected[dt] = protected_doc_maker(dt)
    invalid_queries[dt] = invalid_doc_query_maker(dt)
    invalid_queries[tbl] = invalid_table_query_maker(tbl)


class NonexistenceError(Exception):
    def __init__(self, msg):
        self.msg = msg
        print msg


class PreqlSyntaxError(Exception):
    def __init__(self, msg):
        self.msg = msg
        print msg


class InvalidOperationError(Exception):
    def __init__(self, msg):
        self.msg = msg
        print msg


class DuplicateIDError(Exception):
    def __init__(self, msg):
        self.msg = msg
        print msg


class TopologyError(Exception):
    def __init__(self, msg):
        self.msg = msg
        print msg


class ValueTypeError(Exception):
    def __init__(self, msg):
        self.msg = msg
        print msg


class LimitsExceededError(Exception):
    def __init__(self, msg):
        self.msg = msg
        print msg


errors = {
    'RethinkDBNonexistence': rethinkDBNonexistence,
    'Nonexistence': nonexistence,
    'Protected': protected,
    'IDDuplicates': {
        'graph': graphIDInUse,
        'index': indexIDInUse
    },
    'MissingFields': {
        'id': missing_ids,
        'graph_topo': missing_fields_graph_topo
    },
    'SyntaxError': invalid_queries,
    'TypeError': {
        'topo': needs_topo
    },
    'property_map_sort': pm_sort_error,
    'limits': limits_exceeded
}

error_classes = {
    'NonexistenceError': NonexistenceError,
    'PreqlSyntaxError': PreqlSyntaxError,
    'InvalidOperationError': InvalidOperationError,
    'DuplicateIDError': DuplicateIDError,
    'TopologyError': TopologyError,
    'ValueTypeError': ValueTypeError,
    'LimitsExceededError': LimitsExceededError
}