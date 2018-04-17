const debug = require("debug")("reader-scripts-kinesis"),
    kinesis = require("kinesis"),
    crypto = require("crypto");

module.exports = {
    createStream: (name, shardCount, options) => {
        return new Promise((resolve, reject) => {
            if (!name) reject("Name is required.");
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
            if (!name) reject("Name is required.");
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
            if (!name) reject("Name is required.");
            if (!partitionKey) partitionKey = crypto.randomBytes(16).toString("hex");
            options = options || {};
            debug(`putting data ${JSON.stringify(record)} into ${name}`);

            record = Buffer.from(JSON.stringify(record)).toString("base64");
            const data = { StreamName: name, Data: record, PartitionKey: partitionKey };
            kinesis.request("PutRecord", data, options, (err, out) => {
                if (err) reject(err);
                resolve(out);
            });
        });
    },

    deleteStream: (name, options) => {
        return new Promise((resolve, reject) => {
            if (!name) reject("Name is required.");
            options = options || {};
            debug(`deleting stream ${name}, ${JSON.stringify(options)}`);

            const data = { StreamName: name };
            kinesis.request("DeleteStream", data, options, (err, out) => {
                if (err) reject(err);
                resolve(out);
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
