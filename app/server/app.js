const debug = require('debug')('KSR:app');
const express = require('express');
const cors = require('cors');
const favicon = require('serve-favicon');
const path = require('path');
const kinesis = require('./kinesis-reader');


////////////////////////////////////////////////////////////////////////////////
//---------------------------- node server ------------------------------------
//\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

function kinesisStreamReader(port = 4000) {
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
     * Will set responses as desired AND END!!!!
     * @param   {Object}                    res
     *          The same response object created in the NodeJS route handler.
     * @param   {*}                         message
     *          Whatever description you want to send to the user in the body of
     *          the response. No body will be sent if no message is defined.
     * @param   {Object.<string, string>}   headers
     *          Key-Value pairs of HTTP headers that will be sent with the
     *          response.
     */
    _base: function (res, message, headers) {
        if (message) {
            // this.setHeader('Content-type', 'text/plain');
            res.write(JSON.stringify(message));
        }
        if (Object.keys(headers).length !== 0) {
            Object.keys(headers).forEach(function (key) {
                res.setHeader(key, headers[key]);
            });
        }
        res.end();
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
function validateQueryParams(schema, request) {
    // initialize return value
    const ret = {};
    ret.badRequest = false;

    // find all required params that are not in the request
    const required = schema.required.filter(required => {
        return request[required] === undefined;
    });
    // if any required attributes were not listed as parameters, that's an error
    if (required.length > 0) {
        ret.badRequest = true;
        ret.missingRequiredParams = required;
    }

    // iterate over the requestParams object, getting every attribute contained
    // therein. this is probably optional, but will help fix typos.
    var invalidQueryParams = Object.keys(request).filter(param => {
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
function getEpochTimestamp(minutes = 10) {
    // calculate the unix timestamp based on the passed duration
    // 960 minutes = 8 hours
    var maxMinutes = 960;
    var minsInMs = Math.min(minutes, maxMinutes) * 60 * 1000;
    return new Date(Date.now() - minsInMs);
}



function filterRecords(query, separatedRecords) {
    // filter the records only if the filter was asked for in the URL query params
    if (query.contactId) {
        separatedRecords = separatedRecords.filter(function (record) {
            const contactId = parseInt(query.contactId);
            try {
                // holy cow this is ugly. i wish people knew how to design json correctly.
                // since there can be two different contact IDs, check them both.
                // but look out! if there's no value it'll look like {key: null} while having a value looks like {key: {long: ###}}
                const contactObj = record.baseEventData["com.incontact.datainfra.events.ContactEvent"].mediaScopeIdentification.contactIdentification;
                return (contactObj.contactId && contactObj.contactId.long === contactId) ||
                    (contactObj.contactIdAlt && contactObj.contactIdAlt.long === contactId);
            } catch (err) {
                return false;
            }
        });
    }
    if (query.agentId) {
        separatedRecords = separatedRecords.filter(function (record) {
            const agentId = parseInt(query.agentId);
            try {
                const agentIdObj = record.baseEventData["com.incontact.datainfra.events.AgentEvent"].agentShiftIdentification.agentIdentification;
                return (agentIdObj.agentId && agentIdObj.agentId.long === agentId) ||
                    (agentIdObj.agentIdAlt && agentIdObj.agentIdAlt.long === agentId);
            } catch (err) {
                return false;
            }
        });
    }
    if (query.serverName) {
        separatedRecords = separatedRecords.filter(function (record) {
            try {
                return record.tenantId.serverName.string.toLowerCase() === query.serverName.toLowerCase();
            } catch (err) {
                return false;
            }
        });
    }
    if (query.tenantId) {
        separatedRecords = separatedRecords.filter(function (record) {
            const tenantId = parseInt(query.tenantId);
            try {
                const tenantIdObj = record.tenantId;
                return (tenantIdObj.tenantId && tenantIdObj.tenantId.long === tenantId) ||
                    (tenantIdObj.tenantIdAlt && tenantIdObj.tenantIdAlt.long === tenantId);
            } catch (err) {
                return false;
            }
        });
    }
    if (query.agentShiftId) {
        const agentShiftId = parseInt(query.agentShiftId);
        separatedRecords = separatedRecords.filter(function (record) {
            try {
                const agentShiftIdObj = record.baseEventData["com.incontact.datainfra.events.AgentEvent"].agentShiftIdentification;
                return (agentShiftIdObj.agentShiftId && agentShiftIdObj.agentShiftId.long === agentShiftId) ||
                    (agentShiftIdObj.agentShiftIdAlt && agentShiftIdObj.agentShiftIdAlt.long === agentShiftId);
            } catch (err) {
                return false;
            }
        });
    }
    return separatedRecords;
}


////////////////////////////////////////////////////////////////////////////////
//-------------------------------- routes -------------------------------------
//\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\


// set the default kinesis stream reader route
function recordsRoute(req, res) {
    // quick sanity check for the user on query params
    // make sure required query params are in query
    const paramStatus = validateQueryParams(schema, req.query);
    debug('param status =', JSON.stringify(paramStatus));
    if (paramStatus.badRequest) {
        Responses.invalidRequest(res, paramStatus);
        return;
    }

    const streamname = req.query.streamname;
    const timestamp = getEpochTimestamp(req.query.duration);
    kinesis.getRecords(streamname, timestamp)
        .then(records => {
            const filteredRecords = filterRecords(req.query, records);
            debug("records:", filteredRecords.length);
            Responses.ok(res, filteredRecords);
        })
        .catch(err => {
            const body = {
                badRequest: true,
                error: err.toString()
            };
            debug(body);
            Responses.invalidRequest(res, body);
        });
}

kinesisStreamReader();