// others' modules
const debug = require('debug')('KSR:app');
const express = require('express');
const cors = require('cors');
const favicon = require('serve-favicon');
const path = require('path');

// my modules
const myKinesisReader = require('kinesisReader');
const myResponses = require('responses');
const myQueryParamValidator = require('queryTools');
const filter = require('objectFilter').filterRecords;


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


////////////////////////////////////////////////////////////////////////////////
//-------------------------------- routes -------------------------------------
//\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\


// set the default kinesis stream reader route
function recordsRoute(req, res) {
    // quick sanity check for the user on query params
    // make sure required query params are in query
    const allowedParams = ['duration', 'streamname', 'contactId', 'agentId',
        'serverName', 'tenantId', 'agentShiftId'];
    const requiredParams = ['streamname'];
    const validator = new myQueryParamValidator(allowedParams, requiredParams);
    const paramStatus = validator.validateParams(req.query);
    debug('param status =', JSON.stringify(paramStatus));
    if (paramStatus.badRequest) {
        myResponses.invalidRequest(res, paramStatus);
        return;
    }

    const streamname = req.query.streamname;
    const timestamp = getEpochTimestamp(req.query.duration);
    myKinesisReader.getRecords(streamname, timestamp)
        .then(records => {
            const filteredRecords = filter(req.query, records);
            debug("records:", filteredRecords.length);
            myResponses.ok(res, filteredRecords);
        })
        .catch(err => {
            const body = {
                badRequest: true,
                error: err.toString()
            };
            debug(body);
            myResponses.invalidRequest(res, body);
        });
}


////////////////////////////////////////////////////////////////////////////////
//---------------------------- node server ------------------------------------
//\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

const port = 4000; // TODO: make this a node param or something
const app = express();
app.use(cors());
app.use(favicon(path.join(__dirname, 'public', 'favicon.ico')));
app.get('/records', recordsRoute);
app.listen(port);
debug(`Listening on Port ${port}....`);

/**
 * Export the Express app so that it can be used by Chai
 */
module.exports = app;