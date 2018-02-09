// Records Controller
// ==============

const debug = require("debug")("reader-controller-records");
const moment = require("moment");
const kinesis = require("../scripts/kinesis");

module.exports = {
    getRecords: function(req, res) {
        debug(`params: ${JSON.stringify(req.params)}`);
        const streamname = req.params.streamname;
        const duration = parseInt(req.params.duration);
        const timestamp = moment()
            .subtract(duration, "minutes")
            .valueOf();
        const records = kinesis.getRecords(streamname, timestamp);
        res.send(records);
    }
};
