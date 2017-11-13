const kinesis = require('./secrets').getKinesis();
const deagg = require('aws-kinesis-agg/kpl-deagg');
const common = require('./resources/common');
let AggregatedRecord = common.loadBuilder();
const debug = require('debug')('KSR:kr');

module.exports = function () {

    // public parts
    return {
        // based off of https://github.com/awslabs/kinesis-aggregation/blob/master/node/node_modules/aws-kinesis-agg/kpl-deagg.js
        deaggregate: function (kinesisRecord, computeChecksums, perRecordParse) {
            /* jshint -W069 */ // suppress warnings about dot notation (use of
            // underscores in protobuf model)
            //
            // we receive the record data as a base64 encoded string
            const recordBuffer = new Buffer(kinesisRecord.Data, 'base64');
            let records = [];

            // first 4 bytes are the kpl assigned magic number
            // https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
            if (recordBuffer.slice(0, 4).toString('hex') === kplConfig[useKplVersion].magicNumber) {
                try {
                    // decode the protobuf binary from byte offset 4 to length-16 (last
                    // 16 are checksum)
                    const protobufMessage = AggregatedRecord.decode(recordBuffer.slice(4, recordBuffer.length - 16));

                    // extract the kinesis record checksum
                    const recordChecksum = recordBuffer.slice(recordBuffer.length - 16, recordBuffer.length).toString('base64');

                    if (computeChecksums === true) {
                        // compute a checksum from the serialised protobuf message
                        const md5 = crypto.createHash('md5');
                        md5.update(recordBuffer.slice(4, recordBuffer.length - 16));
                        const calculatedChecksum = md5.digest('base64');

                        // validate that the checksum is correct for the transmitted
                        // data
                        if (calculatedChecksum !== recordChecksum) {
                            // debug("Record Checksum: " + recordChecksum);
                            // debug("Calculated Checksum: " + calculatedChecksum);
                            throw new Error("Invalid record checksum");
                        }
                    }

                    // iterate over each User Record in order
                    for (let i = 0; i < protobufMessage.records.length; i++) {
                        const item = protobufMessage.records[i];

                        // emit the per-record callback with the extracted partition
                        // keys and sequence information
                        const record = perRecordParse(item.data.toString('base64'));
                        records.push(record);
                    }
                } catch (e) {
                }
            }
            else {
                // not a KPL encoded message - no biggie - emit the record with
                // the same interface as if it was. Customers can differentiate KPL
                // user records vs plain Kinesis Records on the basis of the
                // sub-sequence number
                // debug("WARN: Non KPL Aggregated Message Processed for DeAggregation: " + kinesisRecord.partitionKey + "-" + kinesisRecord.sequenceNumber);
                const record = perRecordParse(kinesisRecord.Data);
                if (record) records.push(record);
            }
            return records;
        },

        filterRecords: function (query, separatedRecords) {
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
        },

        getShardIterator: function (params) {
            return new Promise((resolve, reject) => {
                kinesis.getShardIterator(params, (err, data) => {
                    if (err) {
                        reject(new InvalidStreamNameException());
                    }
                    else {
                        resolve(data.ShardIterator);
                    }
                });
            });
        },

        getRecords: function (shardIterator, query) {
            return new Promise((resolve, reject) => {
                const params = {
                    ShardIterator: shardIterator,
                    Limit: 100 // TODO: evaluate this value
                };
                kinesis.getRecords(params, (err, data) => {
                    if (err) {
                        reject(new CannotGetRecordsException("Try again?"));
                    }
                    else {
                        // const records = parseData(data, query);
                        resolve(data);
                    }
                });
            });
        },

        parseData: function (data, query) {
            let deaggregatedList = [];
            data.Records.forEach((aggregateRecord) => {
                // manually deaggregate records
                let separatedRecords = deaggregate(aggregateRecord, false, getRecordAsJson);
                separatedRecords = filterRecords(query);
                // combine lists
                deaggregatedList = deaggregatedList.concat(separatedRecords);
                // Array.prototype.push.apply(deaggregatedList, separatedRecords);
            });

            // if we're not up to date or there are more records being posted, get more records
            // TODO: why do we need the length check??
            if (data.MillisBehindLatest !== 0 || data.Records.length !== 0) {
                module.exports.getRecords(data.NextShardIterator, query)
                    .then((records) => {
                        deaggregatedList = deaggregatedList.concat(records);
                    });
            }
            return deaggregatedList;
        }
    };
}();

// exceptions
// https://stackoverflow.com/questions/464359/custom-exceptions-in-javascript
function InvalidStreamNameException(message) {
    this.message = message;
    Error.captureStackTrace(this, InvalidStreamNameException);
}
InvalidStreamNameException.prototype = Object.create(Error.prototype);
InvalidStreamNameException.prototype.name = "InvalidStreamNameException";
InvalidStreamNameException.prototype.constructor = InvalidStreamNameException;
function CannotGetRecordsException(message) {
    this.message = message;
    Error.captureStackTrace(this, CannotGetRecordsException);
}
CannotGetRecordsException.prototype = Object.create(Error.prototype);
CannotGetRecordsException.prototype.name = "CannotGetRecordsException";
CannotGetRecordsException.prototype.constructor = CannotGetRecordsException;
