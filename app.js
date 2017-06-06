var http = require('http');
var favicon = require('serve-favicon')
var path = require('path')
var finalhandler = require('finalhandler')
var deagg = require('aws-kinesis-agg/kpl-deagg');
var url = require('url');
var common = require('./resources/common');
var kinesis = require('./secrets').getKinesis();

// global variable to allow query params to be accessable from all functions
var query;
// allowed query params: duration, streamname, contactId, agentId, serverName
var allowedQueryParams = ["duration", "streamname", "contactId", "agentId", "serverName"]
// global variable to make the response accessable anywhere
var response;
// global AggregatedRecord object which will hold the protocol buffer model
var AggregatedRecord = common.loadBuilder();


// -------------- NODE SERVER --------------
var _favicon = favicon(path.join(__dirname, 'public', 'favicon.ico'));

var app = http.createServer(function (req, res) {
    // this extra stuff is to allow serving of the favicon.
    // if we don't serve a favicon, we have to explicitly ignore it (which then means the browser doens't cache it, and we keep getting requests for it)
    var done = finalhandler(req, res);
    _favicon(req, res, function (err) {
        if (err) return done(err);

        // quick sanity check for the user on query params
        var queryCheck = Object.keys(url.parse(req.url, true).query);
        // filter out any allowed query params to see if there are disallowed params
        queryCheck = queryCheck.filter(function (q) {
            return allowedQueryParams.indexOf(q) < 0;
        });
        if (queryCheck.length > 0) {
            res.setHeader('Content-type', 'text/plain');
            res.write("The following query parameters are not recognized:\n" + queryCheck);
            res.end();
            console.log('bad query params');
            return;
        }

        var params = processRequest(req);
        response = res;
        setResponse(params);
    });
});
app.listen(4000);
console.log('Listening on Port 4000....');

var processRequest = function (req) {
    console.log('YES!!!!!!!!! New Request received: ' + req.url);

    query = url.parse(req.url, true).query;

    // calculate the timestamp based on the passed duration
    timeAgoInSeconds = query.duration * 60;
    time = Math.floor(Date.now() / 1000) - timeAgoInSeconds;

    return {
        ShardId: '0', /* required */
        ShardIteratorType: 'AT_TIMESTAMP', /* required */
        Timestamp: time,
        StreamName: query.streamname,
    }
}


// -------------- AWS SECTION --------------

var setResponse = function (params) {
    try {
        kinesis.getShardIterator(params, afterShardIterator);
    }
    catch (e) {
        console.log(e, e.stack);
        response.setHeader('Content-type', 'text/plain');
        response.write("Invalid stream: " + query.streamname + "\nOR I've no clue whats going on.");
        response.end();
    }
}

var afterShardIterator = function (err, data) {
    if (!err) {
        if (data.ShardIterator != '') {
            params = {
                ShardIterator: data.ShardIterator, /* required */
                Limit: 100
            };
            kinesis.getRecords(params, handleKinesisRecords);
        } else {
            response.setHeader('Content-type', 'text/plain');
            response.write("No Shard Iterator received");
            response.end();
        }
    }
    else {
        console.log(err, err.stack);
        response.setHeader('Content-type', 'text/plain');
        response.write("Invalid stream: " + query.streamname + "\nOR I've no clue whats going on.");
        response.end();
    }
}

var handleKinesisRecords = function (err, data) {
    if (!err) {
        var allRecords = data.Records;
        var deaggregatedList = []
        for (var i = 0; i < allRecords.length; ++i) {
            aggregateRecord = allRecords[i];
            // manually deaggregate records
            var separatedRecords = deaggregate(aggregateRecord, false, getRecordAsJson);
            // filter the records only if the filter was asked for in the URL query params
            if (query.contactId) {
                separatedRecords = separatedRecords.filter(function (record) {
                    var contactId = parseInt(query.contactId);
                    try {
                        // holy cow this is ugly. i wish people knew how to design json correctly.
                        // since there can be two different contact IDs, check them both.
                        // but look out! if there's no value it'll look like {key: null} while having a value looks like {key: {long: ###}}
                        var contactObj = record.baseEventData["com.incontact.datainfra.events.ContactEvent"].mediaScopeIdentification.contactIdentification;
                        return (contactObj.contactId && contactObj.contactId.long === contactId) ||
                            (contactObj.contactIdAlt && contactObj.contactIdAlt.long === contactId);
                    } catch (err) {
                        return false;
                    }
                });
            }
            if (query.agentId) {
                separatedRecords = separatedRecords.filter(function (record) {
                    var agentId = parseInt(query.agentId);
                    try {
                        var agentIdObj = record.baseEventData["com.incontact.datainfra.events.AgentEvent"].agentShiftIdentification.agentIdentification;
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
            // combine lists
            Array.prototype.push.apply(deaggregatedList, separatedRecords);
        }
    }

    //console.log('Ganesh Your Final data is :  '+jsonResponse);
    if (deaggregatedList.length !== 0) {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.write(JSON.stringify(deaggregatedList));
    }
    else {
        response.writeHead(200, { "Content-Type": "text/plain" });
        response.write("Stream " + query.streamname + " exists, but there were no records found in it with your specified params.");
    }
    response.end();
}

// based off of https://github.com/awslabs/kinesis-aggregation/blob/master/node/node_modules/aws-kinesis-agg/kpl-deagg.js
var deaggregate = function (kinesisRecord, computeChecksums, perRecordCallback, afterRecordCallback) {
    "use strict";
    /* jshint -W069 */ // suppress warnings about dot notation (use of
    // underscores in protobuf model)
    //
    // we receive the record data as a base64 encoded string
    var recordBuffer = new Buffer(kinesisRecord.Data, 'base64');
    var records = [];

    // first 4 bytes are the kpl assigned magic number
    // https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
    if (recordBuffer.slice(0, 4).toString('hex') === kplConfig[useKplVersion].magicNumber) {
        try {
            if (!AggregatedRecord) {
                AggregatedRecord = common.loadBuilder();
            }

            // decode the protobuf binary from byte offset 4 to length-16 (last
            // 16 are checksum)
            var protobufMessage = AggregatedRecord.decode(recordBuffer.slice(4, recordBuffer.length - 16));

            // extract the kinesis record checksum
            var recordChecksum = recordBuffer.slice(recordBuffer.length - 16, recordBuffer.length).toString('base64');

            if (computeChecksums === true) {
                // compute a checksum from the serialised protobuf message
                var md5 = crypto.createHash('md5');
                md5.update(recordBuffer.slice(4, recordBuffer.length - 16));
                var calculatedChecksum = md5.digest('base64');

                // validate that the checksum is correct for the transmitted
                // data
                if (calculatedChecksum !== recordChecksum) {
                    if (debug) {
                        console.log("Record Checksum: " + recordChecksum);
                        console.log("Calculated Checksum: " + calculatedChecksum);
                    }
                    throw new Error("Invalid record checksum");
                }
            } else {
                if (debug) {
                    console.log("WARN: Record Checksum Verification turned off");
                }
            }

            if (debug) {
                console.log("Found " + protobufMessage.records.length + " KPL Encoded Messages");
            }

            // iterate over each User Record in order
            for (var i = 0; i < protobufMessage.records.length; i++) {
                try {
                    var item = protobufMessage.records[i];

                    // emit the per-record callback with the extracted partition
                    // keys and sequence information
                    var record = perRecordCallback(null, {
                        partitionKey: protobufMessage["partition_key_table"][item["partition_key_index"]],
                        explicitPartitionKey: protobufMessage["explicit_hash_key_table"][item["explicit_hash_key_index"]],
                        sequenceNumber: kinesisRecord.sequenceNumber,
                        subSequenceNumber: i,
                        data: item.data.toString('base64')
                    });
                    records.push(record);
                } catch (e) {
                }
            }
        } catch (e) {
        }
    } else {
        // not a KPL encoded message - no biggie - emit the record with
        // the same interface as if it was. Customers can differentiate KPL
        // user records vs plain Kinesis Records on the basis of the
        // sub-sequence number
        if (debug) {
            console.log("WARN: Non KPL Aggregated Message Processed for DeAggregation: " + kinesisRecord.partitionKey + "-" + kinesisRecord.sequenceNumber);
        }
        var record = perRecordCallback(null, {
            partitionKey: kinesisRecord.PartitionKey,
            // explicitPartitionKey : kinesisRecord.explicitPartitionKey,
            sequenceNumber: kinesisRecord.SequenceNumber,
            data: kinesisRecord.Data
        });
        if (record) records.push(record);
    }
    return records;
};

var getRecordAsJson = function (err, singleRecord) {
    if (!err) {
        // foreach
        var entry = new Buffer(singleRecord.data, 'base64').toString();
        try {
            return JSON.parse(entry);
        } catch (ex) {
            console.log("invalid json:\n" + entry);
        }
    }
};