var express = require('express');
var favicon = require('serve-favicon');
var path = require('path');
var url = require('url');
var kinesis = require('./kinesis-reader');

// allowed query params: duration, streamname, contactId, agentId, serverName
var allowedQueryParams = ["duration", "streamname", "contactId", "agentId", "serverName"];
// required query params: streamname
var requiredQueryParams = ["streamname"]


// -------------- NODE SERVER --------------
var app = express();

// set the favicon route
app.use(favicon(path.join(__dirname, 'public', 'favicon.ico')));

// set the default kinesis route (will allow alternate routes in the future and block invalid routes)
app.get('/records', function (req, res) {
    console.log(new Date() + '\n*****NEW REQUEST*****: ' + req.url);

    // quick sanity check for the user on query params
    // make sure required query params are in query
    var requiredQueryCheck = requiredQueryParams.filter(function (queryParam) {
        return req.query[queryParam] === undefined;
    });
    if (requiredQueryCheck.length > 0) {
        res.setHeader('Content-type', 'text/plain');
        res.write("The following query parameters are required:\n" + requiredQueryCheck);
        res.end();
        console.log('missing required query params: ' + requiredQueryCheck);
        return;
    }
    // ensure all params are allowed (this is probably optional, but will help prevent typos)
    var queryCheck = Object.getOwnPropertyNames(req.query).filter(function (queryParam) {
        return allowedQueryParams.indexOf(queryParam) < 0;
    });
    if (queryCheck.length > 0) {
        res.setHeader('Content-type', 'text/plain');
        res.write("The following query parameters are not recognized:\n" + queryCheck);
        res.end();
        console.log('bad query params: ' + queryCheck);
        return;
    }

    // params seem good, dive into AWS stuff
    var query = url.parse(req.url, true).query;
    var params = processRequest(query);
    getResponse(params, query, res);
});

// start the server
app.listen(4000);
console.log('Listening on Port 4000....');

var processRequest = function (query) {
    // calculate the timestamp based on the passed duration
    var timeAgoInMilliseconds = query.duration * 60 * 1000;
    var time = new Date(Date.now() - timeAgoInMilliseconds);

    return {
        ShardId: '0', /* required */
        ShardIteratorType: 'AT_TIMESTAMP', /* required */
        Timestamp: time,
        StreamName: query.streamname,
    }
}

var getResponse = function (params, query, response) {

    kinesis.getRecords(params, query)
        .then(function (deaggregatedList) {
            if (deaggregatedList.length !== 0) {
                response.writeHead(200, { "Content-Type": "application/json" });
                response.write(JSON.stringify(deaggregatedList));
            }
            else {
                response.writeHead(200, { "Content-Type": "text/plain" });
                response.write("Stream " + query.streamname + " exists, but there were no records found in it with your specified params.");
            }
        })
        .catch(function (e) {
            console.log(e, e.stack);
            response.setHeader('Content-type', 'text/plain');
            response.write("Invalid stream: " + query.streamname + "\nOR I've no clue whats going on.");
        })
        .then(function (e) {
            response.end();
        });
};