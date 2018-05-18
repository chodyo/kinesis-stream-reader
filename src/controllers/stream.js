// Stream Controller
// =================

const debug = require("debug")("reader-controller-stream"),
    kinesis = require("kinesis"),
    md5 = require("md5");

function getOptions(query) {
    const options = process.env.NODE_ENV === "production"
        ? {}
        : { host: "localhost", port: 4567 };
    if (query.region) {
        options.region = query.region;
    }
    return options;
}

module.exports = {
    listStreams: function(req, res) {
        debug("listStreams endpoint hit");
        const options = getOptions(req.query);
        kinesis.listStreams(options, function(err, streams) {
            if (err) {
                debug("listStreams error:", err);
                res.status(500).send("Could not get streams.");
            } else {
                streams = streams || [];
                debug("listStreams count:", streams.length);
                res.send(streams);
            }
        });
    },

    describeStreamSummary: function(req, res) {
        debug("describeStreamSummary endpoint hit");
        const data = { StreamName: req.params.name };
        const options = getOptions(req.query);
        kinesis.request("DescribeStream", data, options, function(err, data) {
            if (err) {
                debug("describeStreamSummary error:", err);
                res
                    .status(404)
                    .send(`Could not get details of {${req.params.name}}`);
            } else {
                res.send(data);
            }
        });
    },

    getRecords: function(req, res) {
        debug("getRecords endpoint hit");
        const options = getOptions(req.query);
        options.name = req.params.name;
        const stream = kinesis.stream(options);

        // stream.getShardIds(function(err, data) {
        //     // res.send({ error: err, data: data });
        //     const shardId = parseInt(data[0].split("-")[1]);
        //     debug(shardId);
        //     stream.getShardIteratorRecords({ id: shardId }, function(
        //         err,
        //         data
        //     ) {
        //         res.send({ error: err, data: data });
        //     });
        // });

        stream.getNextRecords(function(err, records) {
            if (err) {
                debug("getRecords error:", err);
                res
                    .status(404)
                    .send(`Could not get records from {${req.params.name}}`);
            } else {
                res.send(records || {});
            }
        });
    },

    createStream: function(req, res) {
        debug("createStream endpoint hit");
        const data = {
            StreamName: req.params.name,
            ShardCount: parseInt(req.params.shardCount)
        };
        const options = getOptions(req.query);
        kinesis.request("CreateStream", data, options, function(err) {
            if (err) {
                debug("createStream error:", err);
                res.status(400).send(err);
            } else {
                res.status(201).send();
            }
        });
    },

    putRecords: function(req, res) {
        debug("putRecords endpoint hit");
        const options = getOptions(req.query);
        const data = {
            Records: req.body.map(record => {
                let data = JSON.stringify(record);
                data = data.padEnd((4 - data.length % 4) + data.length);
                return {
                    Data: data,
                    PartitionKey: md5(record)
                };
            }),
            StreamName: req.params.name,
        };
        kinesis.request("PutRecords", data, options, function(err) {
            if (err) {
                debug("putRecords error:", err);
                res.status(400).send(err);
            } else {
                res.status(201).send();
            }
        });
        // const stream = kinesis.stream(options);
        // stream._write(req.body, "UTF-8", function(err, data) {
        //     if (err) {
        //         debug("putRecords error:", err);
        //         res.status(400).send(err);
        //     } else {
        //         res.status(201).send(data);
        //     }
        // });
    },

    readRecords: function(req, res) {
        debug("readRecords endpoint hit");
        const options = getOptions(req.query);
        options.name = req.params.name;
        const stream = kinesis.stream(options);
        stream.getNextRecords(function(err, data) {
            if (err) {
                debug("readRecords error:", err);
                res.status(400).send(err);
            } else {
                res.send(data);
            }
        });
    },

    deleteStream: function(req, res) {
        debug("deleteStream endpoint hit");
        const data = {
            StreamName: req.params.name
        };
        const options = getOptions(req.query);
        kinesis.request("DeleteStream", data, options, function(err) {
            if (err) {
                debug("deleteStream error:", err);
                res.status(400).send(err);
            } else {
                res.status(200).send();
            }
        });
    }
};
