const debug = require("debug")("reader-scripts-common");
var ProtoBuf = require("protobufjs");
const {
    DEBUG,
    PROTOFILE,
    USE_KPL_VERSION,
    KPL_CONFIG
} = require("./constants");

module.exports.loadBuilder = function() {
    if (DEBUG) {
        debug("Loading Protocol Buffer Model from " + PROTOFILE);
    }

    // create the builder from the proto file
    var builder = ProtoBuf.loadProtoFile(__dirname + "/" + PROTOFILE);
    return builder.build(KPL_CONFIG[USE_KPL_VERSION].messageName);
};
