const debug = require("debug")("reader-scripts-kinesis");
const kinesis = require("kinesis");

module.exports = {
    expose: () => kinesis,

    listStreams: options => {
        debug("getting streams");
        options = options || {};

        return new Promise((resolve, reject) => {
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
