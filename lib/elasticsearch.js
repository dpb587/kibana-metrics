var http = require('http');
var config, logger;
var i = 0;

function createRequest(j, request, response) {
    var headers = request.headers;

    // gzip
    delete headers['accept-encoding'];

    if ('OPTIONS' == request.method) {
        // nginx cors requirement
        headers['content-length'] = '0';
    }

    return http.request(
        {
            host : config[0],
            port : config[1],
            method : request.method,
            path : request.url,
            headers : headers,
            agent : false
        },
        function (u_response) {
            response.writeHead(u_response.statusCode, u_response.headers);

            u_response.on(
                'data',
                function (chunk) {
                    logger.silly('elasticsearch.forward[' + j + ']', '< ' + chunk.toString('utf8'));

                    response.write(chunk);
                }
            );
            u_response.on(
                'end',
                function () {
                    logger.verbose('elasticsearch.forward[' + j + ']', '= upstream closed');

                    response.end();
                }
            );
        }
    );
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

    forward : function (request, response) {
        i += 1;
        var j = i;

        logger.verbose('elasticsearch.forward[' + j + ']', ' = upstream opened (' + request.method + ' ' + request.url + ')');

        var u_request = createRequest(j, request, response);

        request.on(
            'data',
            function (chunk) {
                logger.silly('elasticsearch.forward[' + j + ']', '> ' + chunk.toString('utf8'));

                u_request.write(chunk);
            }
        );

        request.on(
            'end',
            function () {
                u_request.end();
            }
        );
    },

    forwardData : function (request, response, data) {
        i += 1;
        var j = i;

        var u_request = createRequest(j, request, response);

        u_request.write(data);
        u_request.end();
    },

    send : function (url, data, callback) {
        i += 1;
        var j = i;

        var u_request = http.request(
            {
                host : config[0],
                port : config[1],
                method : 'POST',
                path : url,
                headers: {
                    'content-type' : 'application/x-www-form-urlencoded',
                    'content-length' : data.length
                }
            },
            function (u_response) {
                var buffer = [];

                u_response.on(
                    'data',
                    function (chunk) {
                        logger.silly('elasticsearch.send[' + j + ']', '< ' + chunk.toString('utf8'));

                        buffer.push(chunk);
                    }
                );

                u_response.on(
                    'end',
                    function () {
                        logger.verbose('elasticsearch.send[' + j + ']', '= upstream closed');

                        callback(
                            (200 > u_response.statusCode) || (300 <= u_response.statusCode),
                            JSON.parse(buffer.join(''))
                        );
                    }
                );
            }
        );

        logger.silly('elasticsearch.send[' + j + ']', '> ' + data.toString('utf8'));

        logger.verbose('elasticsearch.send[' + j + ']', '= upstream opened');

        u_request.write(data);
        u_request.end();
    }
}

module.exports = _;
