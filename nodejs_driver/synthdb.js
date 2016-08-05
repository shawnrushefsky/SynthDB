// SynthDB Node.js Driver
// Made available under the MIT License (MIT)
// Copyright (c) 2016 Psymphonic LLC
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
// Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

var request = require('request');
var r = require('rethinkdb');
var fs = require('fs');
var streamify = require('stream-generators');
var csv = require('fast-csv');
var stream = require('stream');
var util = require('util');

module.exports = function(){
    String.prototype.format = function () {
      var i = 0, args = arguments;
      return this.replace(/\{}/g, function () {
          return typeof args[i] != 'undefined' ? args[i++] : '';
      });
    };
    
    function capitalize(string) {
        return string.charAt(0).toUpperCase() + string.slice(1);
    }

    function camelCase(text){
        text = text.split('_');
        let newText = text[0];
        for(let i = 1; i < text.length; i++){
            newText += capitalize(text[i]);
        }
        return newText;
    }
    
    function Connection(options){
        var me = this;
        this.host = options.host;
        this.api = me.host+'preql/q';
        this.key = options.key;
        this.verify = options.verify;
        this.delim = "\t";
        this.update = me.host+"preql/update";

        function stream_json (iterable, verbose){
            return function*(){
                verbose = verbose || false;
                if(verbose){
                    for(let item of iterable){
                        console.log(item);
                        yield JSON.stringify(item)+me.delim;
                    }
                }
                else{
                    for(let item of iterable){
                        yield JSON.stringify(item)+me.delim;
                    }
                }
            };
        }

        var post_catch = function(url, data, headers, callback, stream){
            var opts = {
                'url': url,
                'headers': headers,
                'strictSSL': me.verify,
                'method': 'POST'
            };
            stream = stream || false;
            if(stream){
                streamify(data).pipe(request(opts, function(err, resp, body){
                    if(!err && resp.statusCode == 200){
                        callback(JSON.parse(body))
                    }
                    else{
                        console.log(err, resp, body);
                        throw "PreqlDriverError"
                    }
                }));
            }
            else{
                opts.body = data;
                request(opts, function(err, resp, body){
                    if(!err && resp.statusCode == 200){
                        callback(JSON.parse(body))
                    }
                    else{
                        console.log(err, resp, body);
                        throw "PreqlDriverError"
                    }
                })
            }
            
            
        };
        
        var put_catch = function(url, data, headers, callback, finished, stream){
            var opts = {
                'url': url,
                'headers': headers,
                'strictSSL': me.verify,
                'method': 'PUT',
                'body': data
            };
            if(stream){
                opts.encoding = null;
                let resp = request(opts);
                var r_stream = new ResponseStream(callback, finished, resp);
            }
            else{
                request(opts, function(err, resp, body){
                    if(!err && resp.statusCode == 200){
                        callback(JSON.parse(body))
                    }
                    else{
                        console.log(err, resp, body);
                        throw "PreqlDriverError"
                    }
                })
            }
            
        };
        

        function ResponseStream(callback, finished, input){
            stream.Writable.call(this);
            var buffer = '';
            var stop = input.close;
            this._write = function(chunk, encoding, next){
                buffer += chunk;
                let i;
                let piece = '';
                let offset = 0;
                while((i = buffer.indexOf(me.delim, offset)) !== -1) {
                    piece = buffer.substr(offset, i - offset);
                    offset = i+1;
                    callback(JSON.parse(piece), stop);
                }
                buffer = buffer.substr(offset);
                next();
            };
            this.on('finish', function(){
                finished()
            });
            input.pipe(this);

        }
        util.inherits(ResponseStream, stream.Writable);

        this.go = function(query, callback, finished, verbose, test){
            var q = query.q;
            var headers = {
                'Api-Key': me.key,
                'g': query.g,
                'q': q
            };
            if(q === "insert"){
                headers.params = JSON.stringify(query.params);
                post_catch(me.api, stream_json(query.body, verbose), headers, callback, true);
            }
            else if(["create_graph", "drop_graph", "list_graphs", "graph_stats"].indexOf(q) > -1){
                post_catch(me.api, null, headers, callback);
            }
            else if(["pluck", "stream", "update", "topology", "generate", "commit", "graph_filter", "delete",
                   "create_index", "walk", "fields"].indexOf(q) > -1){
                headers.params = JSON.stringify(query.params);
                put_catch(me.api, JSON.stringify(query.body), headers, callback, finished, query.stream);
            }
            else if(q === "server_update"){
                post_catch(me.update, null, headers, callback);
            }
            else{
                console.log("QUERY NOT SUPPORTED", query);
            }
        }
    }

    function connect(options, callback){
        var host, verify;
        if(typeof options === "function"){
            host = "http://127.0.0.1:7796";
            verify = false;
            callback = options;
        }
        else{
            host = options.host || "http://127.0.0.1:7796";
            verify = options.verify || false;
        }
        if(host.indexOf("http://") === -1 && host.indexOf("https://") === -1){
            host = "http://" + host;
        }
        if(host.substr(host.length-1) !== "/"){
            host += "/";
        }
        if(options.key_file) {
            fs.readFile(options.key_file, 'utf8', function (err, data) {
                if (err) {
                    return console.log(err);
                }
                var opts = {
                    'url': host+'preql/q',
                    'headers': {
                        'Api-Key': data,
                        'q': 'ping'
                    },
                    'strictSSL': verify
                };
                request(opts, function(err, resp, body){
                    if(body === "Hi there!"){
                        callback(new Connection({host: host, key: data, verify: verify}))
                    }
                })
            })
        }
    }

    function Runnable(){
        var me = this;
        this.params = {};
        this.q = null;
        this.stream = false;
        this.body = {};
        this.g = null;

        this.run = function(conn, callback, done, options){
            options = options || {};
            let verbose, test;
            done = done || function(){};
            if(options.hasOwnProperty('verbose')){
                verbose = options.verbose;
            }
            else{
                verbose = false;
            }
            if(options.hasOwnProperty('test')){
                test = options.test;
            }
            else{
                test = false;
            }
            conn.go(me, callback, done, verbose, test);
        };

        this.extends = function(object_params){
            for(let k in object_params){
                if(object_params.hasOwnProperty(k)){
                    me[k] = object_params[k];
                }
            }
        }
    }

    function GeneratorFuncs(){
        var ret = {};
        var generators = ['price_network', 'random_graph', 'triangulation', 'lattice', 'complete_graph',
            'circular_graph', 'geometric_graph'];
        var gen_func_maker = function(gen_type){
            return function(options){
                options = options || null;
                var nq = this._clone();
                nq.q = "generate";
                nq.body.gen_type = gen_type;
                nq.body.gen_params = options;
                nq.queryString += ".{}({})".format(gen_type, JSON.stringify(options));
                return nq
            }
        };
        for(let i = 0; i < generators.length; i++){
            ret[camelCase(generators[i])] = gen_func_maker(generators[i]);
        }
        return ret;
    }

    function createGraph(graph_id){
        Runnable.call(this);
        var me = this;
        this.g = graph_id;
        this.q = "create_graph";
        this.queryString = "create_graph('{}')".format(graph_id);
        this._clone = function(){
            var p = new createGraph(me.g);
            p.queryString = me.queryString;
            p.params = me.params;
            p.body = me.body;
            p.q = me.q;
            p.stream = me.stream;
            return p;
        };
        this.extends(GeneratorFuncs());
    }
    
    function dropGraph(graph_id){
        Runnable.call(this);
        this.g = graph_id;
        this.q = "drop_graph";
    }

    function listGraphs(){
        Runnable.call(this);
        this.q = "list_graphs";
    }
    
    function isIterable(obj){
        return obj instanceof ([]).constructor || obj instanceof (function*(){}).constructor
    }

    function update(){
        Runnable.call(this);
        this.q = "server_update";
    }

    function unique(arr) {
        var u = {}, a = [];
        for(let item of arr){
            if(!u.hasOwnProperty(item)) {
                a.push(item);
                u[item] = 1;
            }
        }
        return a;
    }
   
    function PreqlExtensions(){
        var ret = {};
        var doc_types = ['node', 'link', 'node_type', 'link_type'];

        var insert_maker = function(doc_type){
            return function(to_insert, options){
                options = options || null;
                var nq = this._clone();
                nq.q = "insert";
                if(to_insert instanceof ([]).constructor){
                    nq.body = to_insert;
                }
                else if(to_insert instanceof (function*(){}).constructor){
                    nq.body = to_insert();
                }
                else{
                    nq.body = [to_insert];
                }
                nq.params.type = doc_type;
                nq.queryString += ".insert{}([{}".format(capitalize(doc_type), nq.body);
                if(options){
                    if(options.hasOwnProperty('conflict')){
                        nq.params.conflict = options.conflict;
                    }
                    if(options.hasOwnProperty('durability')){
                        nq.params.durability = options.durability;
                    }
                    nq.queryString += "], {})".format(JSON.stringify(options));
                }
                else{
                    nq.queryString += '])';
                }
                return nq;
            }
        };
        var doc_select_maker = function(doc_type){
            return function(obj_id){
                var nq = this._clone();
                nq.q = "pluck";
                nq.params.type = doc_type;
                nq.params.obj_id = obj_id;
                nq.stream = false;
                nq.queryString += ".{}({})".format(camelCase(doc_type), obj_id);
                return nq
            }
        };
        var stream_maker = function(doc_type){
            return function(get_all, options){
                get_all = get_all || null;
                if(get_all instanceof (function*(){}).constructor){
                    get_all = get_all();
                }
                else if(!(get_all instanceof ([]).constructor || get_all instanceof ("").constructor)){
                    options = get_all;
                    get_all = null;
                }
                options = options || null;

                var nq = this._clone();
                nq.q = 'stream';
                nq.params.type = doc_type;
                nq.stream = true;
                if(options){
                    if(options.hasOwnProperty("index")){
                        nq.body.index = options.index;
                    }
                    if(options.hasOwnProperty('uids') && options.uids){
                        nq.body.index = "uid";
                    }
                }
                if(doc_type === "property_map"){
                    nq.params.obj_id = get_all;
                    nq.body.property_map = get_all;
                    nq.queryString += ".{}('{}')".format(camelCase(doc_type), get_all);
                    return nq
                }
                else if(doc_type === "array"){
                    nq.params.obj_id = get_all;
                    nq.body.array = get_all;
                    nq.queryString += ".{}('{}')".format(doc_type, get_all);
                    return nq
                }
                if(get_all){
                    if(isIterable(get_all)){
                        nq.body.get_all = unique(get_all);
                    }
                    else{
                        nq.body.get_all = [get_all];
                    }
                }
                nq.queryString += ".{}(".format(doc_type);
                if(nq.body.hasOwnProperty("get_all")){
                    nq.queryString += JSON.stringify(nq.body.get_all);
                    if(options){
                        nq.queryString += ", {})".format(JSON.stringify(options));
                    }
                    else{
                        nq.queryString += ")";
                    }
                }
                else{
                    nq.queryString += ")";
                }
                return nq;
                
            }
        };
        var topo_maker = function(topo_query){
            return function(options){
                options = options || {};
                var nq = this._clone();
                for(let k in options){
                    if(options.hasOwnProperty(k) && options[k] instanceof (function(){}).constructor){
                        nq.body.js_func = true;
                        options[k] = "({})".format(options[k].toString());
                    }
                }
                
                if(nq.q !== "walk"){
                    nq.q = "topology";
                    nq.stream = false;
                    nq.body.topo_params = options;
                    nq.body.topo = topo_query;
                }
                else{
                    nq.body.walk_rules.topo_params = options;
                    nq.body.walk_rules.topo = topo_query;
                }
                
                nq.queryString += ".{}({})".format(topo_query, JSON.stringify(options));
                return nq
            }
        };
        var walk_maker = function(walk_type){
            return function(options){
                options = options || {};
                var nq = this._clone();
                nq.q = "walk";
                nq.body.alg = walk_type;
                nq.body.walk_rules = options;
                if(options.hasOwnProperty('filters')){
                    for(let i = 0; i < options.filters.length; i++){
                        if(options.filters[i].hasOwnProperty('node') && (options.filters[i].node instanceof (function(){}).constructor)){
                            nq.body.js_func = true;
                            
                            options.filters[i].node = "({})".format(options.filters[i].node.toString());
                        }
                        if(options.filters[i].hasOwnProperty('link') && (options.filters[i].link instanceof (function(){}).constructor)){
                            nq.body.js_func = true;
                            options.filters[i].link = "({})".format(options.filters[i].link.toString());
                        }
                    }
                }
                nq.queryString += ".{}({})".format(walk_type, JSON.stringify(options));
                return nq;
            }
        };
        var table_types = [];
        for(let dt of doc_types){
            ret[camelCase(dt)] = doc_select_maker(dt);
            table_types.push(dt+'s');
        }
        for(let tt of table_types){
            ret["insert"+capitalize(tt)] = insert_maker(tt);
        }
        var stream_types = table_types.concat(['property_map', 'array', 'property_maps', 'arrays']);
        for(let st of stream_types){
            ret[camelCase(st)] = stream_maker(st);
        }
        var topo_queries = ['pagerank', 'betweenness', 'closeness', 'eigenvector', 'katz', 'hits', 'hits_hub', 'hits_authority',
                'central_point_dominance', 'eigentrust', 'trust_transitivity', 'sfdp', 'fruchterman_reingold',
                'arf', 'radial_tree', 'random_layout', 'shortest_distance', 'shortest_path', 'pseudo_diameter',
                'is_bipartite', 'is_planar', 'is_DAG', 'max_cardinality_matching', 'max_independent_node_set',
                'link_reciprocity', 'sequential_node_coloring', 'similarity', 'isomorphism',
                'subgraph_isomorphism', 'min_spanning_tree', 'dominator_tree',
                'topological_sort', 'kcore_decomposition', 'tsp_tour', 'random_spanning_tree', 'all_links',
                'out_links', 'in_links', 'all_neighbors', 'in_degree', 'out_degree', 'in_neighbors', 'out_neighbors',
                'origin', 'terminus'];
        for(let tq of topo_queries){
            ret[camelCase(tq)] = topo_maker(tq);
        }
        var walkers = ['breadth_first', 'depth_first'];
        for(let wf of walkers){
            ret[camelCase(wf)] = walk_maker(wf);
        }
        return ret;
    }

    function PreqlQuery(graph_id){
        var me = this;
        Runnable.call(this);
        this.g = graph_id;
        this.queryString = "graph('{}')".format(me.g);
        this.params.type = "graph";
        this.q = "graph_stats";

        this._clone = function(){
            var p = new PreqlQuery(me.g);
            p.queryString = me.queryString;
            p.params = me.params;
            p.body = me.body;
            p.q = me.q;
            p.stream = me.stream;
            return p;
        };
        
        this.extends(PreqlExtensions());

        this.createIndex = function(index_name, definition){
            definition = definition || null;
            var nq = me._clone();
            nq.q = "create_index";
            nq.body.index_name = index_name;
            if(definition instanceof (function(){}).constructor){
                definition = "({})".format(definition.toString());
                nq.body.js_func = true;
            }
            nq.body.definition = definition;
            nq.stream = false;
            nq.queryString += ".create_index('{}', {})".format(index_name, definition);
            return nq;
        };

        this.update = function(upd){
            var nq = me._clone();
            nq.q = "update";
            nq.body.update = upd;
            nq.queryString += ".update({})".format(upd);
            nq.stream = false;
            return nq;
        };

        this.delete = function(){
            var nq = me._clone();
            nq.q = "delete";
            nq.stream = false;
            nq.queryString += ".delete()";
            return nq;
        };

        this.get = function(obj_id){
            var nq = me._clone();
            nq.q = 'pluck';
            nq.body.select = obj_id;
            nq.stream = false;
            nq.queryString += ".get({})".format(obj_id);
            return nq;
        };

        this.getAll = function(get_all, options) {
            get_all = get_all || null;
            if (get_all instanceof (function*() {
                }).constructor) {
                get_all = get_all();
            }
            else if (!(get_all instanceof ([]).constructor || get_all instanceof ("").constructor)) {
                options = get_all;
                get_all = null;
            }
            options = options || null;
            var nq = me._clone();
            nq.body.get_all = get_all;
            nq.stream = true;
            if(options){
                if(options.hasOwnProperty("index")){
                    nq.body.index = options.index;
                }
                if(options.hasOwnProperty('uids') && options.uids){
                    nq.body.index = "uid";
                    nq.body.uids = true;
                }
                nq.queryString += ".getAll({}, {})".format(nq.body['get_all'], JSON.stringify(options));
            }
            else{
                nq.queryString += ".getAll({})".format(nq.body['get_all']);
            }
            return nq;
        };
        
        this.filter = function(predicate){
            var nq = this._clone();
            if(nq.q === "graph_stats"){
                nq.q = "graph_filter";
                nq.stream = false
            }
            if(predicate instanceof (function(){}).constructor){
                predicate = "({})".format(predicate.toString());
                nq.body.js_func = true;
                nq.queryString += ".filter{}".format(predicate);
            }
            else{
                nq.queryString += ".filter({})".format(predicate);
            }
            nq.body.filter = predicate;
            
            return nq;
        };

        this.limit = function(number){
            var nq = this._clone();
            nq.body.limit = number;
            if(number <= 1){
                nq.stream = false;
            }
            nq.queryString += ".limit({})".format(number);
            return nq
        };

        this.map = function(mapper){
            var nq = this._clone();
            nq.body.js_func = true;
            if(nq.q !== "walk"){
                mapper = "({})".format(mapper.toString());
                nq.body.map = mapper;
                nq.queryString += ".map{}".format(mapper);
            }
            else{
                if(mapper.hasOwnProperty('nodes')){
                    let mf = mapper.nodes.toString()+"(node)";
                    let rf = "function(node){return [node.id, {}]}".format(mf);
                    nq.body.walk_rules.nmap = "({})".format(rf.toString())
                }
                if(mapper.hasOwnProperty('links')){
                    let mf = mapper.links.toString()+"(link)";
                    let rf = "function(link){return [link.id, {}]}".format(mf);
                    nq.body.walk_rules.lmap = "({})".format(rf)
                }
            }
            
            return nq;
        };

        this.sort = function(options){
            var nq = this._clone();
            if(nq.q !== "walk"){
                if(options.hasOwnProperty('order_by') && options.order_by instanceof (function(){}).constructor){
                    options.order_by = "({})".format(options.order_by.toString());
                }
                nq.body.sort = options;
            }
            else{
                nq.body.walk_rules.sort = options;
            }
            if(!(nq.body.hasOwnProperty('coerce_to') && nq.body.coerce_to === 'array')){
                nq.stream = true;
            }
            nq.queryString += ".sort({})".format(JSON.stringify(options));
            return nq;
        };
        
        this.allFields = function(){
            var nq = this._clone();
            nq.q = "fields";
            nq.queryString += ".allFields()";
            return nq
        };

        this.count = function(){
            var nq = this._clone();
            nq.body.count = true;
            nq.queryString += ".count()";
            nq.stream = false;
            return nq;
        };

        this.reduce = function(reducer){
            var nq = this._clone();
            reducer = "({})".format(reducer.toString());
            if(nq.q !== "walk"){
                nq.body.reduce = reducer;
            }
            else{
                nq.body.walk_rules.reduce = reducer;
            }
            nq.body.js_func = true;
            nq.stream = false;
            nq.queryString += ".reduce{}".format(reducer);
            return nq;
        };

        this._ = function(field){
            var nq = this._clone();
            if(!nq.body.hasOwnProperty('nested')){
                nq.body.nested = [];
            }
            nq.body.nested.push(field);
            nq.queryString += "._({})".format(field);
            return nq;
        };

        this.coerceTo = function(data_type, options){
            options = options || null;
            var nq = this._clone();
            nq.body.coerce_to = data_type;
            nq.stream = data_type === "stream";
            if(data_type === "property_map"){
                if(options){
                    if(options.hasOwnProperty('name')){
                        nq.body.pmap_name = options.name;
                    }
                    if(options.hasOwnProperty('type')){
                        nq.body.pmap_type = options.type;
                    }
                    nq.queryString += ".coerceTo('{}', {})".format(data_type, JSON.stringify(options));
                }
            }
            nq.queryString += ".coerceTo('{}')".format(data_type);
            return nq;
        };

        this.commit = function(fields){
            var nq = this._clone();
            nq.q = "commit";
            nq.stream = false;
            nq.body.commit_target = fields;
            nq.queryString += ".commit({})".format(JSON.stringify(fields));
            return nq;
        };

        this.walkOut = function(options){
            options = options || {};
            var nq = this._clone();
            nq.q = "walk";
            if(nq.body.hasOwnProperty('walk_rules')){
                nq.body.walk_rules.direction.push("out");
                nq.body.walk_rules.distance += 1;
            }
            else{
                nq.body.alg = "breadth_first";
                nq.body.walk_rules = {
                    direction: ['out'],
                    distance: 1,
                    filters: []
                };
            }
            nq.body.walk_rules.filters.push(options);
            for(let k in options){
                if(options.hasOwnProperty(k) && options[k] instanceof (function(){}).constructor){
                    options[k] = "({})".format(options[k].toString());
                    nq.body.js_func = true;
                }
            }
            nq.queryString += ".walk_out({})".format(JSON.stringify(options));
            return nq;
        };
        
        this.walkIn = function(options){
            options = options || {};
            var nq = this._clone();
            nq.q = "walk";
            if(nq.body.hasOwnProperty('walk_rules')){
                nq.body.walk_rules.direction.push("in");
                nq.body.walk_rules.distance += 1;
            }
            else{
                nq.body.alg = "breadth_first";
                nq.body.walk_rules = {
                    direction: ['in'],
                    distance: 1,
                    filters: []
                };
            }
            nq.body.walk_rules.filters.push(options);
            for(let k in options){
                if(options.hasOwnProperty(k) && options[k] instanceof (function(){}).constructor){
                    options[k] = "({})".format(options[k].toString());
                    nq.body.js_func = true;
                }
            }
            nq.queryString += ".walk_in({})".format(JSON.stringify(options));
            return nq;
        };

        this.i = this.walkIn;

        this.o = this.walkOut;
        
    }

    function graph(graph_id){
        return new PreqlQuery(graph_id);
    }
    
    // function* csv_unicode(filename, options){
    //     let stream = fs.createReadStream(filename);
    //     csv.fromStream(stream, options).on('data', function(data){
    //         yield data
    //     })
    // }
    //
    // function* csv_json(filename, headers, options){
    //     let stream = fs.createReadStream(filename);
    //     options.headers = headers;
    //     csv
    //         .fromStream(stream, options)
    //         .on('data', function(data){
    //             yield data
    //         })
    // }

    return {
        "connect": connect,
        "graph": graph,
        "field": r.row,
        "expr": r.expr,
        "literal": function(obj){
            return "##r.literal({})##".format(JSON.stringify(obj));
        },
        // "csv_unicode": csv_unicode,
        // "csv_json": csv_json,
        "createGraph": function(graph_id){
            return new createGraph(graph_id);
        },
        "dropGraph": function(graph_id){
            return new dropGraph(graph_id);
        },
        "listGraphs": function(){
            return new listGraphs();
        },
        "update": function(){
            return new update();
        }
    }
}();