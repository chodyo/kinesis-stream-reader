const debug = require('debug')('KSR:app');
const express = require('express');
const cors = require('cors');
const favicon = require('serve-favicon');
const path = require('path');
const url = require('url');
const kinesis = require('./kinesis-reader');


//////////////////////////////////////////////////////////////
// --------------------- node server ------------------------
////////////////////////////////////////////////////////////

const app = express();
app.use(cors());
app.use(favicon(path.join(__dirname, 'public', 'favicon.ico')));


//////////////////////////////////////////////////////////////
// ---------------------- responses -------------------------
////////////////////////////////////////////////////////////

const invalidRequestResponse = function (res, message = null, headers = {}) {
    res.statusCode = 400;
    if (message) {
        res.setHeader('Content-type', 'text/plain');
        Object.keys(headers).forEach(function (key) {
            res.setHeader(key, headers[key]);
        });
        res.write(message);
    }
};

//////////////////////////////////////////////////////////////
// ------------------- query parameters ---------------------
////////////////////////////////////////////////////////////

/**
 * @typedef     {Object}    QueryParamSchema
 * @property    {string[]}  schema.allowed      
 *              A list of all valid query parameters. 
 * @property    {string[]}  schema.required        
 *              A list of all required query parameters. 
 */
const schema = {
    allowed: [
        'duration',
        'streamname',
        'contactId',
        'agentId',
        'serverName',
        'tenantId',
        'agentShiftId'
    ],
    required: [
        'streamname'
    ]
};

/**
 * @typedef     {Object}    QueryParamValidator
 * @property    {bool}      goodRequest
 *              Indicates whether or not the parameters given are valid and in 
 *              the correct form.
 * @property    {string[]}  missingRequiredParams
 *              All parameters that should have been in the request but weren't.
 * @property    {string[]}  invalidParams
 *              All parameters that were given and not recognized.
 */
/**
 * Ensures all required query params exist, and all existing params are valid.
 * @param   {QueryParamSchema}  schema                   
 *          The query parameter schema.
 * @param   {string[]}          requestParams          
 *          A list of all given query parameters for a request. 
 * @return  {QueryParamValidator}            
 *          Whether or not query params were valid and reasons why the query
 *          params were invalid, if applicable.
 */
const validateQueryParams = function (schema, requestParams) {
    // initialize return value
    const ret = {};
    ret.goodRequest = true;

    // find missing params
    const required = schema.required.filter(function (param) {
        return requestParams[param] === undefined;
    });
    if (required) {
        ret.goodRequest = false;
        ret.missingRequiredParams = required;
    }

    // ensure all params are allowed (this is probably optional, but will help prevent typos)
    var invalidQueryParams = Object.getOwnPropertyNames(requestParams).filter(
        function (param) {
            return requestParams[allowed].indexOf(param) < 0;
        }
    );
    if (invalidQueryParams) {
        ret.goodRequest = false;
        ret.invalidParams = invalidQueryParams;
    }

    return ret;
}

//////////////////////////////////////////////////////////////
// ----------------------- routes ---------------------------
////////////////////////////////////////////////////////////


// set the default kinesis route (will allow alternate routes in the future and block invalid routes)
app.get('/records', function (req, res) {
    debug(new Date() + ': ' + req.url);

    // quick sanity check for the user on query params
    // make sure required query params are in query
    debug("res type: " + typeof (res));


    // params seem good, dive into AWS stuff
    var query = url.parse(req.url, true).query;
    var params = processRequest(query);
    getResponse(params, query, res);
});

// start the server
app.listen(4000);
debug('Listening on Port 4000....');

var processRequest = function (query) {
    // calculate the timestamp based on the passed duration
    // put a (hopefully temporary?) hard cap on duration of 960 minutes (8 hours) to avoid getting wrecked
    var maxDuration = 960;
    var duration = Math.min(query.duration, maxDuration);
    if (query.duration > maxDuration) debug('Limiting duration to ' + maxDuration + '.');
    var timeAgoInMilliseconds = duration * 60 * 1000;
    var time = new Date(Date.now() - timeAgoInMilliseconds);

    return {
        ShardId: '0', /* required */
        ShardIteratorType: 'AT_TIMESTAMP', /* required */
        Timestamp: time,
        StreamName: query.streamname
    };
};

var getResponse = function (params, query, response) {

    kinesis.getRecords(params, query)
        .then(function (deaggregatedList) {
            debug('Returning ' + deaggregatedList.length + ' records.');
            if (deaggregatedList.length !== 0) {
                response.writeHead(200, { 'Content-Type': 'application/json' });
                response.write(JSON.stringify(deaggregatedList));
            }
            else {
                response.writeHead(200, { 'Content-Type': 'text/plain' });
                response.write('Stream ' + query.streamname + ' exists, but there were no records found in it with your specified params.');
            }
        })
        .catch(function (e) {
            debug(e, e.stack);
            response.writeHead(400, { 'Content-type': 'text/plain' });
            response.write('Invalid stream: ' + query.streamname + '\nOR I have no clue whats going on.');
        })
        .then(function () {
            response.end();
        });
};