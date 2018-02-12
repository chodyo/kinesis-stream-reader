// Stream Controller
// =================

const debug = require("debug")("reader-controller-stream"),
    kinesis = require("kinesis");

function getOptions() {
    return process.env.NODE_ENV === "production"
        ? {}
        : { host: "localhost", port: 4567 };
}

module.exports = {
    listStreams: function(req, res) {
        debug("listStreams endpoint hit");
        const options = getOptions();
        if (req.query.region) {
            options.region = req.query.region;
        }
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
        const options = getOptions();
        if (req.query.region) {
            options.region = req.query.region;
        }
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
        const options = getOptions();
        options.name = req.params.name;
        if (req.query.region) {
            options.region = req.query.region;
        }
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
    }
};
