const kinesis = require('./secrets').getKinesis();
const deagg = require('aws-kinesis-agg/kpl-deagg');
const common = require('./resources/common');
let AggregatedRecord = common.loadBuilder();
const debug = require('debug')('KSR:kr');

module.exports = function () {
    // public parts
    return {
        getRecords: function (streamname, timestamp) {
            return new Promise((resolve, reject) => {
                // 1. get the first shard iterator (kinesis.getShardIterator)
                let getShardIteratorInput = getAwsData(streamname, timestamp);
                kinesis.getShardIterator(getShardIteratorInput, (err, data) => {
                    if (err) {
                        reject(new InvalidStreamNameException());
                    }
                    else {
                        resolve(data.ShardIterator);
                    }
                });
            }).then(shardIterator => {


                // 2. get data from the current shard iterator (kinesis.getRecords)
                let waitingForResponse = false;
                let data = [];
                while (true) {
                    if (!waitingForResponse) {
                        waitingForResponse = true;
                        const getRecordsInput = {
                            ShardIterator: shardIterator,
                            Limit: 100 // TODO: evaluate this value
                        };
                        kinesis.getRecords(getRecordsInput, (err, response) => {
                            if (err) {
                                reject(new CannotGetRecordsException("Try again?\n" + err.toString()));
                            }
                            // 2.5 add all data from amazon to the data list
                            data = data.concat(response.Records);

                            // 3. if amazon returns us data with `MillisBehindLatest` greater than 0 or maybe if there are current records, go back to step 2
                            if (response.MillisBehindLatest !== 0 || response.Records.length !== 0) {
                                waitingForResponse = false;
                                getRecordsInput.ShardIterator = response.NextShardIterator;
                            }
                            else {
                                // 4. parse the data and return the records as a list
                                return data;
                            }
                        });
                    }
                }
            }).then(unparsedRecords => {
                let deaggregatedList = [];
                unparsedRecords.Records.forEach((aggregateRecord) => {
                    // manually deaggregate records
                    let separatedRecords = deaggregate(aggregateRecord, false, getRecordAsJson);
                    separatedRecords = filterRecords(query);
                    // combine lists
                    deaggregatedList = deaggregatedList.concat(separatedRecords);
                    // Array.prototype.push.apply(deaggregatedList, separatedRecords);
                });

                // if we're not up to date or there are more records being posted, get more records
                // TODO: why do we need the length check??

                return deaggregatedList;
            });
        }
    };
}();

// based off of https://github.com/awslabs/kinesis-aggregation/blob/master/node/node_modules/aws-kinesis-agg/kpl-deagg.js
const deaggregate = function (kinesisRecord, computeChecksums, perRecordParse) {
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
};

const filterRecords = function (query, separatedRecords) {
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
};

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
        ShardId: '0',
        ShardIteratorType: 'AT_TIMESTAMP',
        StreamName: stream,
        Timestamp: oldestRecord
    };
};

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
function NoShardIteratorException(message) {
    this.message = message;
    Error.captureStackTrace(this, NoShardIteratorException);
}
NoShardIteratorException.prototype = Object.create(Error.prototype);
NoShardIteratorException.prototype.name = "NoShardIteratorException";
NoShardIteratorException.prototype.constructor = NoShardIteratorException;

const getRecordJson = function (data) {
    const entry = new Buffer(data, 'base64').toString();
    try {
        return JSON.parse(entry);
    } catch (ex) {
        return { "INVALID JSON": entry };
    }
};
