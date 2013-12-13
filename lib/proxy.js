var http = require('http');
var config, logger;
var server, elasticsearch, graphite;
var i = 0;

function handleRequest(request, response, data) {
    var json = JSON.parse(data);
    var forwarderCount = 0;
    var forwarder = {
        graphite : {}
    };

    if ('facets' in json) {
        for (var facet in json['facets']) {
            if ('object' == typeof json['facets'][facet].date_histogram) {
                var m = json['facets'][facet].facet_filter.fquery.query.filtered.query.query_string.query.match(/^@graphite:([^:]+)$/);
                var r;

                json['facets'][facet].facet_filter.fquery.query.filtered.filter.bool.must.some(
                    function (v) {
                        if ('object' == typeof v.range) {
                            for (var n in v.range) break;

                            r = v.range[n];
                        
                            return true;
                        }
                    }
                );

                if (m && r) {
                    forwarderCount += 1;

                    var tmp = {
                        facet : facet,
                        target : m[1],
                        interval : graphite.convertElasticsearchInterval(json['facets'][facet].date_histogram.interval),
                        from : new Date(10 * Math.ceil(r.from / 10)),
                        to : ('now' == r.to) ? new Date() : new Date(r.to)
                    };
                    var tmpkey = tmp.interval + '_' + tmp.from.getTime() + '_' + tmp.to.getTime() + '_' + forwarderCount;

                    forwarder.graphite[tmpkey] = forwarder.graphite[tmpkey] || [];
                    forwarder.graphite[tmpkey].push(tmp);

                    // drop it from the elasticsearch request
                    delete json['facets'][facet];
                }
            }
        }

        if (0 < forwarderCount) {
            forwarderCount = 0;
            var knownWrapper;
            var knownFacets = {};

            function handleResponse() {
                for (var k in knownFacets) {
                    knownWrapper.facets[k] = knownFacets[k];
                }

                var data = JSON.stringify(knownWrapper);

                response.writeHead(
                    200,
                    'OK',
                    {
                        'access-control-allow-origin' : '*',
                        'content-type' : 'application/json; charset=UTF-8',
                        'content-length' : data.length
                    }
                );

                response.write(data);
                response.end();
            }

            Object.keys(forwarder.graphite).forEach(
                function (k) {
                    var targets = {};
                    var lastgk;

                    forwarder.graphite[k].forEach(
                        function (gk) {
                            targets['summarize(' + gk.target + ', "' + gk.interval + '", "min")'] = [ gk.facet, 'min' ];
                            targets['summarize(' + gk.target + ', "' + gk.interval + '", "max")'] = [ gk.facet, 'max' ];
                            targets['summarize(' + gk.target + ', "' + gk.interval + '", "avg")'] = [ gk.facet, 'mean' ];
                            targets['summarize(' + gk.target + ', "' + gk.interval + '", "sum")'] = [ gk.facet, 'total' ];

                            lastgk = gk;
                        }
                    );

                    forwarderCount += 1;

                    graphite.send(
                        lastgk.from,
                        lastgk.to,
                        Object.keys(targets),
                        function (err, data) {
                            var res1 = {};

                            for (var k0 in data) {
                                var tref = targets[k0];
                                res1[tref[0]] = res1[tref[0]] || {};

                                for (var k1 in data[k0]) {
                                    res1[tref[0]][k1] = res1[tref[0]][k1] || {};
                                    res1[tref[0]][k1][tref[1]] = data[k0][k1];
                                }
                            }

                            for (var facet in res1) {
                                var res2 = [];

                                for (var ts in res1[facet]) {
                                    res1[facet][ts].time = 1000 * ts;
                                    res1[facet][ts].count = 1;
                                    res1[facet][ts].total_count = 1;

                                    res2.push(res1[facet][ts]);
                                }

                                knownFacets[facet] = {
                                    '_type' : 'date_histogram',
                                    'entries' : res2
                                };
                            }

                            forwarderCount -= 1;

                            if (0 == forwarderCount) {
                                handleResponse();
                            }
                        }
                    );
                }
            );

            if (0 < Object.keys(json['facets']).length) {
                forwarderCount += 1;

                elasticsearch.send(
                    request.url,
                    JSON.stringify(json),
                    function (err, data) {
                        knownWrapper = data;
                        forwarderCount -= 1;

                        if (0 == forwarderCount) {
                            handleResponse();
                        }
                    }
                );
            } else {
                knownWrapper = {
                    took : 1,
                    timed_out : false,
                    _shards : {
                        total : 1,
                        successful : 1,
                        failed : 0
                    },
                    hits : {
                        total : 1,
                        max_score : 1,
                        hits : []
                    },
                    facets : {}
                };
            }

            return;
        }
    }

    elasticsearch.forwardData(request, response, data);
}

var _ = {
    setConfig : function (sconfig) {
        config = sconfig.split(':');

        return this;
    },

    setLogger : function (slogger) {
        logger = slogger;

        return this;
    },

    setElasticsearch : function (selastcisearch) {
        elasticsearch = selastcisearch;

        return this;
    },

    setGraphite : function (sgraphite) {
        graphite = sgraphite;

        return this;
    },

    run : function () {
        server = http.createServer();

        server.on(
            'request',
            function (request, response) {
                i += 1;
                var j = i;
                var buffer = [];

                logger.verbose('proxy.run[' + j + ']', '= downstream opened (' + request.socket.remoteAddress + ':' + request.socket.remotePort + '; ' + request.method + ' ' + request.url + ')');

                response.on(
                    'finish',
                    function () {
                        logger.verbose('proxy.run[' + j + ']', '= downstream closed');
                    }
                );

                if (!request.url.match(/^\/logstash-/)) {
                    logger.verbose('proxy.run[' + j + ']', '= forwarding via path');

                    elasticsearch.forward(request, response)
                } else {
                    request.on(
                        'data',
                        function (chunk) {
                            logger.silly('proxy.run[' + j + ']', '< ' + chunk.toString('utf8'));

                            buffer.push(chunk);
                        }
                    );

                    request.on(
                        'end',
                        function () {
                            if (0 == buffer.length) {
                                logger.verbose('proxy.run[' + j + ']', '= forwarding via no content');

                                elasticsearch.forwardData(request, response, '');
                            } else {
                                handleRequest(request, response, buffer.join(''));
                            }
                        }
                    );
                }
            }
        );

        server.on(
            'listening',
            function () {
                logger.info('proxy.run', '= listening on ' + this.address().address + ':' + this.address().port);
            }
        );

        server.listen(config[1], config[0]);
    }
}

module.exports = _;
