const kinesis = require('./secrets').getKinesis();
const deagg = require('aws-kinesis-agg/kpl-deagg');
const common = require('./resources/common');
let AggregatedRecord = common.loadBuilder();
const debug = require('debug')('KSR:kr');

module.exports = {
    getRecords: getRecords
};

async function getRecords(streamname, timestamp) {
    // 1. get the first shard iterator (kinesis.getShardIterator)
    let shardIteratorInput = getAwsData(streamname, timestamp);
    let shardIterator = await getShardIterator(shardIteratorInput);

    // 2. get data from the current shard iterator (kinesis.getRecords)
    // will be the accumulated data
    let parsedRecords = [];
    // variable to be changed by sequential calls
    let getRecordsInput = {
        ShardIterator: shardIterator,
        Limit: 100
    };
    while (getRecordsInput) {
        const results = await getKinesisRecords(getRecordsInput);
        // 2.5 add all data from amazon to the data list
        // unparsedRecords = unparsedRecords.concat(results.Records);

        results.Records.forEach(aggregateRecord => {
            // manually deaggregate records
            let separatedRecords = deaggregate(aggregateRecord, false, getRecordAsJson);
            // combine lists
            parsedRecords = parsedRecords.concat(separatedRecords);
            // Array.prototype.push.apply(deaggregatedList, separatedRecords);
        });
        // 3. if amazon returns us data with `MillisBehindLatest` greater than 0 or maybe if there are current records, go back to step 2
        // if we're not up to date or there are more records being posted, get more records
        // TODO: why do we need the length check??
        if (results.MillisBehindLatest !== 0 || results.Records.length !== 0) {
            getRecordsInput.ShardIterator = results.NextShardIterator;
        }
        else {
            getRecordsInput = null;
        }
    }

    return parsedRecords;
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
function getAwsData(stream, oldestRecord) {
    return {
        ShardId: '0',
        ShardIteratorType: 'AT_TIMESTAMP',
        StreamName: stream,
        Timestamp: oldestRecord
    };
}

function getShardIterator(getShardIteratorInput) {
    return new Promise((resolve, reject) => {
        kinesis.getShardIterator(getShardIteratorInput, (err, data) => {
            if (err) {
                reject(new InvalidStreamNameException());
            }
            else {
                resolve(data.ShardIterator);
            }
        });
    });
}


function getKinesisRecords(getRecordsInput) {
    return new Promise((resolve, reject) => {
        kinesis.getRecords(getRecordsInput, function (err, data) {
            if (err) {
                promise = null;
                reject(CannotGetRecordsException("Try again?\n" + err.toString()));
            }
            resolve(data);
        });
    });
}

function getRecordAsJson(data) {
    const entry = new Buffer(data, 'base64').toString();
    try {
        return JSON.parse(entry);
    } catch (ex) {
        return { "INVALID JSON": entry };
    }
}

// based off of https://github.com/awslabs/kinesis-aggregation/blob/master/node/node_modules/aws-kinesis-agg/kpl-deagg.js
function deaggregate(kinesisRecord, computeChecksums, perRecordParse) {
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
}


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

