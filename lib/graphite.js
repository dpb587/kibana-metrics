var http = require('http');
var config, logger;
var i = 0;

var _ = {
    setConfig : function (sconfig) {
        config = sconfig.split(':');

        return this;
    },

    setLogger : function (slogger) {
        logger = slogger;

        return this;
    },

    formatGraphiteDate : function (d) {
        // 04:00_20110501
        return ((d.getUTCHours() < 10) ? '0' : '') + d.getUTCHours()
            + ':' + ((d.getUTCMinutes() < 10) ? '0' : '') + d.getUTCMinutes()
            + '_' + d.getUTCFullYear()
            + ((d.getUTCMonth() < 9) ? '0' : '') + (1 + d.getUTCMonth())
            + ((d.getUTCDate() < 10) ? '0' : '') + d.getUTCDate()
            ;
    },

    convertElasticsearchInterval : function (v) {
        var x;

        if (x = v.match(/^(\d+)s$/)) {
            return x[1] + 'sec';
        } else if (x = v.match(/^(\d+)m$/)) {
            return x[1] + 'min';
        } else if (x = v.match(/^(\d+)h$/)) {
            return x[1] + 'hour';
        } else if (x = v.match(/^(\d+)d$/)) {
            return x[1] + 'day';
        }

        throw new Error('Unable to convert elasticsearch time interval');
    },

    send : function (from, to, targets, callback) {
        i += 1;
        var j = i;

        var path = encodeURI(
            '/render?tz=UTC&from=' + _.formatGraphiteDate(from)
                + '&to=' + _.formatGraphiteDate(to)
                + '&format=json&target='
                + targets.join('&target=')
            );

        var u_request = http.request(
            {
                host : config[0],
                port : config[1],
                method : 'GET',
                path : path
            },
            function (u_response) {
                var buffer = [];

                u_response.on(
                    'data',
                    function (chunk) {
                        logger.silly('graphite.send[' + j + ']', '< ' + chunk);

                        buffer.push(chunk);
                    }
                );

                u_response.on(
                    'end',
                    function () {
                        logger.verbose('graphite.send[' + j + ']', '= upstream closed');

                        var data = JSON.parse(buffer.join(''));
                        var result = {};

                        for (var k0 in data) {
                            var v = data[k0].datapoints;
                            var datamap = {};

                            for (var k1 in v) {
                                datamap[v[k1][1]] = v[k1][0];
                            }

                            result[data[k0].target] = datamap;
                        }

                        callback(false, result);
                    }
                );
            }
        );

        logger.verbose('graphite.send[' + j + ']', '= upstream opened (GET ' + path + ')');

        u_request.end();
    }
};

module.exports = _;
