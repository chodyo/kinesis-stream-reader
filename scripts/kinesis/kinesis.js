const debug = require("debug")("reader-scripts-kinesis");
const kinesis = require("kinesis");

module.exports = {
    expose: () => kinesis,

    createStream: (name, shardCount, options) => {
        return new Promise((resolve, reject) => {
            if (!name) reject("Name is required.");
            shardCount = shardCount || 1;
            options = options || {};
            debug(`creating stream ${name}, ${shardCount}, ${JSON.stringify(options)}`);

            const data = { StreamName: name, ShardCount: shardCount };
            kinesis.request("CreateStream", data, options, err => {
                if (err) reject(`Stream ${name} could not be created: ${err}`);
                resolve(data);
            });
        });
    },

    getStreamInfo: (name, options) => {
        return new Promise((resolve, reject) => {
            if (!name) reject("Name is required.");
            options = options || {};
            debug(`getting stream info ${name}, ${JSON.stringify(options)}`);

            const data = { StreamName: name };
            kinesis.request("DescribeStream", data, options, (err, data) => {
                if (err) reject(`Stream ${name} could not be described: ${err}`);
                resolve(data);
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
                if (err) reject(`Stream ${name} could not be deleted: ${err}`);
                resolve(out);
            });
        });
    },

    listStreams: options => {
        return new Promise((resolve, reject) => {
            options = options || {};
            debug(`getting streams with options ${JSON.stringify(options)}`);

            kinesis.listStreams(options, (err, streams) => {
                if (err) {
                    debug("listStreams error:", err);
                    reject(err);
                } else {
                    debug("listStreams count:", streams.length);
                    streams = streams || [];
                    debug("listStreams count:", streams.length);
                    resolve(streams);
                }
            });
        });
    }
};
