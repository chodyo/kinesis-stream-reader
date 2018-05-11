const debug = require("debug")("reader-scripts-kinesis"),
    kinesis = require("kinesis"),
    crypto = require("crypto");

module.exports = {
    createStream: (name, shardCount, options) => {
        return new Promise((resolve, reject) => {
            if (!name) reject("Streamname is required.");
            shardCount = shardCount || 1;
            options = options || {};
            debug(`creating stream ${name}, ${shardCount}, ${JSON.stringify(options)}`);

            const data = { StreamName: name, ShardCount: shardCount };
            kinesis.request("CreateStream", data, options, (err, out) => {
                if (err) reject(err);
                resolve(out);
            });
        });
    },

    getStreamInfo: (name, options) => {
        return new Promise((resolve, reject) => {
            if (!name) reject("Streamname is required.");
            options = options || {};
            debug(`getting stream info ${name}, ${JSON.stringify(options)}`);

            const data = { StreamName: name };
            kinesis.request("DescribeStream", data, options, (err, out) => {
                if (err) reject(err);
                resolve(out);
            });
        });
    },

    putRecord: (name, record, partitionKey, options) => {
        return new Promise((resolve, reject) => {
            if (!name) reject("Streamname is required.");
            partitionKey = partitionKey || crypto.randomBytes(16).toString("hex");
            options = options || {};
            debug(`putting data ${JSON.stringify(record)} into ${name}`);

            record = serialize(record);
            const data = { StreamName: name, Data: record, PartitionKey: partitionKey };
            kinesis.request("PutRecord", data, options, (err, out) => {
                if (err) reject(err);
                resolve(out);
            });
        });
    },

    putRecords: (name, records, partitionKey, options) => {
        return new Promise((resolve, reject) => {
            debug(`putting data ${JSON.stringify(records)} into ${name}`);
            if (!name) reject("Streamname is required.");
            records = records.map(record => {
                return {
                    Data: serialize(record),
                    PartitionKey: partitionKey || crypto.randomBytes(16).toString("hex")
                };
            });
            options = options || {};

            const data = { StreamName: name, Records: records };
            debug(`putting data ${JSON.stringify(data)} into ${name}`);
            kinesis.request("PutRecords", data, options, (err, out) => {
                if (err) reject(err);
                resolve(out);
            });
        });
    },

    getRecords: (name, shardIteratorType, shardId, params, options) => {
        return new Promise((resolve, reject) => {
            if (!name) reject("Streamname is required.");
            if (!shardIteratorType) reject("ShardIteratorType is required.");
            if (!shardId) shardId = "0";
            options = options || {};

            const iteratorData = { ShardId: shardId, ShardIteratorType: shardIteratorType, StreamName: name };
            switch (shardIteratorType) {
                case "AT_SEQUENCE_NUMBER":
                case "AFTER_SEQUENCE_NUMBER":
                    if (!params.StartingSequenceNumber)
                        reject(`params.StartingSequenceNumber is required when shardIteratorType=${shardIteratorType}`);
                    iteratorData.StartingSequenceNumber = params.StartingSequenceNumber;
                    break;
                default:
                    reject("Unknown ShardIteratorType");
                    break;
            }

            function kinesisRecordsFetcher(shardIterator) {
                return new Promise((resolve, reject) => {
                    kinesis.request("GetRecords", { ShardIterator: shardIterator }, options, (err, response) => {
                        if (err) reject(err);
                        resolve(response);
                    });
                });
            }

            kinesis.request("GetShardIterator", iteratorData, options, async (err, response) => {
                debug(`getShardIterator response: ${JSON.stringify(response)}`);
                if (err) reject(err);
                const allRecords = [];
                let isBehindLatest = 1;
                let shardIterator = response.ShardIterator;
                while (isBehindLatest !== 0 && shardIterator !== null) {
                    const response = await kinesisRecordsFetcher(shardIterator);
                    debug(`GetRecords response: ${JSON.stringify(response)}`);
                    shardIterator = response.NextShardIterator;
                    isBehindLatest = response.MillisBehindLatest;
                    response.Records.forEach(record => allRecords.push(deserialize(record.Data)));
                }
                resolve(allRecords);
            });
        });
    },

    deleteStream: (name, options) => {
        return new Promise((resolve, reject) => {
            if (!name) reject("Streamname is required.");
            options = options || {};
            debug(`deleting stream ${name}, ${JSON.stringify(options)}`);

            const data = { StreamName: name };
            kinesis.request("DeleteStream", data, options, err => {
                if (err) reject(err);
                resolve(true);
            });
        });
    },

    listStreams: options => {
        return new Promise((resolve, reject) => {
            options = options || {};
            debug(`getting streams with options ${JSON.stringify(options)}`);

            kinesis.listStreams(options, (err, streams) => {
                if (err) reject(err);
                resolve(streams || []);
            });
        });
    }
};

function serialize(record) {
    return Buffer.from(JSON.stringify(record)).toString("base64");
}

function deserialize(record) {
    return JSON.parse(Buffer.from(record, "base64").toString());
}
