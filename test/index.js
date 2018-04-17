const expect = require("chai").expect,
    debug = require("debug")("reader-tests"),
    kinesalite = require("kinesalite"),
    myKinesis = require("../scripts/kinesis");

const kinesaliteServer = kinesalite({
        path: "./mydb",
        createStreamMs: 50,
        ssl: true
    }),
    myKinesisOptions = { host: "localhost", port: 4567 };

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
                    debug("streams:", streams);
                    expect(streams).to.have.length(0);
                    done();
                })
                .catch(err => {
                    debug("err:", err);
                    done(err);
                });
        });
    });

    describe("with existing streams", function() {
        const newStreamNames = ["one", "two", "three"];

        before(() => {
            return new Promise((resolve, reject) => {
                newStreamNames.forEach(newStreamName => {
                    myKinesis
                        .createStream(newStreamName, 1, myKinesisOptions)
                        .then(() => {
                            debug(`stream ${newStreamName} created`);
                            resolve();
                        })
                        .catch(err => {
                            debug(`could not delete stream ${newStreamName}: ${err}`);
                            reject(err);
                        });
                });
            });
        });

        after(() => {
            newStreamNames.forEach(streamName => {
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
                    debug("streams:", streams);
                    expect(streams).to.have.length(newStreamNames.length);
                    expect(streams).to.have.same.members(newStreamNames);
                    done();
                })
                .catch(err => {
                    debug("err:", err);
                    done(err);
                });
        });
    });
});
