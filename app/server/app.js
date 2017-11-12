const debug = require('debug')('KSR:app');
const express = require('express');
const cors = require('cors');
const favicon = require('serve-favicon');
const path = require('path');
const kinesis = require('./kinesis-reader');


////////////////////////////////////////////////////////////////////////////////
//---------------------------- node server ------------------------------------
//\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

const kinesisStreamReader = function (port = 4000) {
    const app = express();
    app.use(cors());
    app.use(favicon(path.join(__dirname, 'public', 'favicon.ico')));
    app.get('/records', recordsRoute);
    app.listen(port);
    debug(`Listening on Port ${port}....`);
}

////////////////////////////////////////////////////////////////////////////////
//----------------------------- responses -------------------------------------
//\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\


const Responses = {
    /**
     * @param   {Object}                    res
     *          The same response object created in the NodeJS route handler.
     * @param   {*}                         [message]
     *          Whatever description you want to send to the user in the body of
     *          the response. No body will be sent if no message is defined.
     * @param   {Object.<string, string>}   [headers]
     *          Key-Value pairs of HTTP headers that will be sent with the
     *          response.
     */
    invalidRequestResponse: function (res, message = null, headers = {}) {
        res.statusCode = 400;
        if (message) {
            // this.setHeader('Content-type', 'text/plain');
            res.write(JSON.stringify(message));
        }
        if (Object.keys(headers).length !== 0) {
            Object.keys(headers).forEach(function (key) {
                res.setHeader(key, headers[key]);
            });
        }
    }
};


////////////////////////////////////////////////////////////////////////////////
//-------------------------- query parameters ---------------------------------
//\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

/**
 * @typedef     {Object}        QueryParamSchema
 * @property    {Set<string>}   schema.allowed
 *              A set of all valid query parameters.
 * @property    {string[]}      schema.required
 *              A list of all required query parameters. Should overlap allowed.
 */
const schema = {
    allowed: new Set([
        'duration',
        'streamname',
        'contactId',
        'agentId',
        'serverName',
        'tenantId',
        'agentShiftId'
    ]),
    required: [
        'streamname'
    ]
};

/**
 * @typedef     {Object}    QueryParamValidator
 * @property    {boolean}   badRequest
 *              Indicates if the parameters given are invalid or in the
 *              incorrect form.
 * @property    {string[]}  [missingRequiredParams]
 *              All parameters that should have been in the request but weren't.
 * @property    {string[]}  [invalidParams]
 *              All parameters that were given and not recognized.
 */
/**
 * Ensures all required query params exist, and all existing params are valid.
 * @param   {QueryParamSchema}  schema
 *          The query parameter schema.
 * @param   {Object}            requestParams
 *          An object containing a property for each query string parameter in
 *          the route. If there is no query string, it is the empty object, {}.
 * @return  {QueryParamValidator}
 *          Whether or not query params were valid and reasons why the query
 *          params were invalid, if applicable.
 */
const validateQueryParams = function (schema, reqParams) {
    // initialize return value
    const ret = {};
    ret.badRequest = false;

    // iterate over schema.required, ensuring that every required param is in
    // the requestParams object
    const required = schema.required.filter(function (param) {
        return reqParams[param] === undefined;
    });
    // if any required attributes were not listed as parameters, that's an error
    if (required.length > 0) {
        ret.badRequest = true;
        ret.missingRequiredParams = required;
    }

    // iterate over the requestParams object, getting every attribute contained
    // therein. this is probably optional, but will help fix typos.
    var invalidQueryParams = Object.keys(reqParams).filter(function (param) {
        // include the attribute if the parameter is not enumerated in the set
        return !schema.allowed.has(param);
    });
    // if any attributes were not specified as allowed params, that's an error
    if (invalidQueryParams.length > 0) {
        ret.badRequest = true;
        ret.invalidParams = invalidQueryParams;
    }

    return ret;
}


////////////////////////////////////////////////////////////////////////////////
//-------------------------------- routes -------------------------------------
//\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\


// set the default kinesis stream reader route
const recordsRoute = function (req, res) {
    debug(new Date() + ': ' + req.url);

    // quick sanity check for the user on query params
    // make sure required query params are in query
    const paramStatus = validateQueryParams(schema, req.query);
    debug('param status =', JSON.stringify(paramStatus));
    if (paramStatus.badRequest) {
        Responses.invalidRequestResponse(res, paramStatus);
        res.end();
        return;
    }


    // params seem good, dive into AWS stuff
    // var query = url.parse(req.url, true).query;
    var params = processRequest(req.query);
    getResponse(params, req.query, res);
}

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

kinesisStreamReader();