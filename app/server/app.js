const debug = require('debug')('KSR:app');
const express = require('express');
const cors = require('cors');
const favicon = require('serve-favicon');
const path = require('path');

const KinesisReader = require('./kinesis-reader');
const Responses = require('./custom_modules/responses');
const QueryParamValidator = require('./custom_modules/query-param-validator');


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
    const allowedParams = ['duration','streamname','contactId','agentId',
        'serverName','tenantId','agentShiftId'];
    const requiredParams = ['streamname'];
    const validator = new QueryParamValidator(allowedParams, requiredParams);
    const paramStatus = validator.validateParams(req.query);
    debug('param status =', JSON.stringify(paramStatus));
    if (paramStatus.badRequest) {
        Responses.invalidRequest(res, paramStatus);
        return;
    }

    const streamname = req.query.streamname;
    const timestamp = getEpochTimestamp(req.query.duration);
    KinesisReader.getRecords(streamname, timestamp)
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