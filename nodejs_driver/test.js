/**
 * Created by psymac0 on 6/27/16.
 */
var s = require('./synthdb');

s.connect(
    {
        host: "https://dev_server",
        key_file: "/Users/psymac0/GitHub/SynthDB/synthdb_node/secure.key"
    },
    function(c){
        var g = s.graph('price3');
        var filter = {'link': {type: 'Link'}};
        g.links()._('type').run(c, function(each){
            console.log(each);
        });
        // let qu = g.node(0)
        //     .walkIn().walkIn()
        //     .similarity({direction: "all"})
        //     .map({nodes: function (node) {
        //         return 1;
        //     }})
        //     .reduce(function(x, y){return x+y})
        //     .sort({reverse: true});
        // var callback = function([id, value]){
        //     console.log(id, value);
        // };
        // var done = function(){
        //     console.log("The stream has ended");
        // };
        // qu.run(c, callback, done);
        
    }
);