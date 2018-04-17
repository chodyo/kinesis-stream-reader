const expect = require("chai").expect,
    debug = require("debug")("reader-tests"),
    kinesalite = require("kinesalite"),
    myKinesis = require("../scripts/kinesis");

const kinesaliteServer = kinesalite({
        path: "./mydb",
        ssl: true,
        createStreamMs: 0
    }),
    myKinesisOptions = {
        host: "localhost",
        port: 4567,
        region: "us-east-1",
        credentials: { accessKeyId: "dummy", secretAccessKey: "dummy" }
    };

describe("My kinesis module", function() {
    before(function() {
        kinesaliteServer.listen(4567, function(err) {
            if (err) throw err;
            debug("Kinesalite server started on port 4567");
        });
    });

    describe("with no existing streams", function() {
        it("can ask for stream names", done => {
            myKinesis
                .listStreams(myKinesisOptions)
                .then(streams => {
                    debug(`streams: ${streams}`);
                    expect(streams).to.have.length(0);
                    done();
                })
                .catch(err => {
                    debug(`err: ${err}`);
                    done(err);
                });
        });
    });

    describe("with existing streams", function() {
        const nonEmptyStreamNames = ["one", "two", "three"];
        const stream = nonEmptyStreamNames[0];
        const data = { test: true };

        before(() => {
            return new Promise((resolve, reject) => {
                nonEmptyStreamNames.forEach(newStreamName => {
                    myKinesis
                        .createStream(newStreamName, 1, myKinesisOptions)
                        .then(() => {
                            debug(`stream ${newStreamName} created`);
                            resolve();
                        })
                        .catch(err => {
                            debug(`could not create stream ${newStreamName}: ${err}`);
                            reject(err);
                        });
                });
            });
        });

        after(() => {
            nonEmptyStreamNames.forEach(streamName => {
                myKinesis
                    .deleteStream(streamName, myKinesisOptions)
                    .then(() => {
                        debug(`stream ${streamName} deleted`);
                    })
                    .catch(err => {
                        debug(`could not delete stream ${streamName}: ${err}`);
                    });
            });
        });

        it("can ask for stream names", done => {
            myKinesis
                .listStreams(myKinesisOptions)
                .then(streams => {
                    debug(`streams: ${streams}`);
                    expect(streams).to.have.length(nonEmptyStreamNames.length);
                    expect(streams).to.have.same.members(nonEmptyStreamNames);
                    done();
                })
                .catch(err => {
                    debug(`err: ${err}`);
                    done(err);
                });
        });

        it("can put data", done => {
            myKinesis
                .putData(stream, data, null, myKinesisOptions)
                .then(() => {
                    debug(`put data ${JSON.stringify(data)} to stream ${stream}`);
                    done();
                })
                .catch(err => {
                    debug(`err: ${err}`);
                    done(err);
                });
        });
    });
});
