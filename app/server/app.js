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

/**
 * Container object for all responses the KSR will return.
 */
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
    _base: function(res, message, headers) {
        if (message) {
            // this.setHeader('Content-type', 'text/plain');
            res.write(JSON.stringify(message));
        }
        if (Object.keys(headers).length !== 0) {
            Object.keys(headers).forEach(function (key) {
                res.setHeader(key, headers[key]);
            });
        }
    },
    /**
     * @augments Responses.base
     */
    invalidRequest: function (res, message = null, headers = {}) {
        res.statusCode = 400;
        this._base(res, message, headers);
    },
    /**
     * @augments Responses.base
     */
    ok: function (res, message = null, headers = {}) {
        res.statusCode = 200;
        this._base(res, message, headers);
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
//--------------------------------- utils -------------------------------------
//\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

/**
 * Get the epoch timestamp from some number of minutes ago. This must be in the
 * form of a JS Date() object. I've never been able to get the AWS SDK to work
 * with just the Unix timestamp value.
 * @param {number} [minutes=10]
 * No record should be older than this number of mins.
 * @returns {Date}
 * The number of ms that has passed in UTC since the epoch.
 */
const getEpochTimestamp = function (minutes = 10) {
    // calculate the unix timestamp based on the passed duration
    // 960 minutes = 8 hours
    var maxMinutes = 960;
    var minutesInMilliseconds = Math.min(minutes, maxMinutes) * 60 * 1000;
    debug(minutesInMilliseconds, Date.now(), new Date(Date.now() - minutesInMilliseconds));
    return new Date(Date.now() - minutesInMilliseconds);
}

/**
 * Parameter object for the [AWS SDK getShardIterator]
 * {@link http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html#getShardIterator-property}
 * operation.
 * @typedef AwsData
 * @property {string} StreamName
 * The name of the Amazon Kinesis stream.
 * @property {string} ShardId
 * The shard ID of the Amazon Kinesis shard to get the iterator for.
 * @property {string} ShardIteratorType
 * Determines how the shard iterator is used to start reading data records from
 * the shard.
 * @property {Date} Timestamp
 * The timestamp of the data record from which to start reading. If a record
 * with this exact timestamp does not exist, the iterator returned is for the
 * next (later) record.
 */
/**
 * Parameterizes data from the request into an AwsData object.
 * @param {string} stream
 * Name of the AWS Kinesis stream.
 * @param {Date} oldestRecord
 * Records should be more recent than this timestamp.
 * @returns {AwsData}
 */
const getAwsData = function (stream, oldestRecord) {
    return {
        ShardId: '0', /* required */
        ShardIteratorType: 'AT_TIMESTAMP', /* required */
        StreamName: stream, /* required */
        Timestamp: oldestRecord
    };
}


////////////////////////////////////////////////////////////////////////////////
//-------------------------------- routes -------------------------------------
//\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

// set the default kinesis stream reader route
const recordsRoute = function (req, res) {
    // quick sanity check for the user on query params
    // make sure required query params are in query
    const paramStatus = validateQueryParams(schema, req.query);
    debug('param status =', JSON.stringify(paramStatus));
    if (paramStatus.badRequest) {
        Responses.invalidRequest(res, paramStatus);
        res.end();
        return;
    }

    // params seem good, dive into AWS stuff
    const awsParams = getAwsData(
        req.query.streamname,
        getEpochTimestamp(req.query.duration)
    );


    kinesis.getRecords(awsParams, req.query)
        .then(function (deaggregatedList) {
            debug('Returning ' + deaggregatedList.length + ' records.');
            Responses.ok(res, deaggregatedList);
        })
        .catch(function (e) {
            debug(e, e.stack);
            res.writeHead(400, { 'Content-type': 'text/plain' });
            res.write('Invalid stream: ' + req.query.streamname + '\nOR I have no clue whats going on.');
        })
        .then(function () {
            res.end();
        });
}

kinesisStreamReader();