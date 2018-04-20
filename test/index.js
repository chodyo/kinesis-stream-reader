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
    const nonEmptyStreamNames = ["one", "two", "three"];

    before(function() {
        return new Promise((resolve, reject) => {
            kinesaliteServer.listen(4567, function(err) {
                if (err) throw err;
                debug("Kinesalite server started on port 4567");
            });
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

    it("can put a record", done => {
        const stream = nonEmptyStreamNames[0],
            data = { test: true };
        myKinesis
            .putRecord(stream, data, null, myKinesisOptions)
            .then(returnObj => {
                debug(`put data ${JSON.stringify(data)} to stream ${stream}; ${JSON.stringify(returnObj)}`);
                expect(returnObj).to.haveOwnProperty("SequenceNumber");
                expect(returnObj).to.haveOwnProperty("ShardId");
                done();
            })
            .catch(err => {
                debug(`err: ${err}`);
                done(err);
            });
    });

    it("can get a record using shard iterator type AT_SEQUENCE_NUMBER", done => {
        const stream = nonEmptyStreamNames[0],
            data = { myData: Math.random() * 0x10000000000000 };
        myKinesis
            .putRecord(stream, data, null, myKinesisOptions)
            .then(returnObj =>
                myKinesis.getRecords(
                    stream,
                    "AT_SEQUENCE_NUMBER",
                    returnObj.ShardId,
                    { StartingSequenceNumber: returnObj.SequenceNumber },
                    myKinesisOptions
                )
            )
            .then(records => {
                debug(`records gotten using shard iterator type AT_SEQUENCE_NUMBER: ${JSON.stringify(records)}`);
                expect(records).to.be.an("array");
                expect(records).to.deep.include.members([JSON.stringify(data)]);
                expect(records).to.have.length(1);
                done();
            })
            .catch(err => {
                debug(`err: ${err}`);
                done(err);
            });
    });

    it("can get multiple records using shard iterator type AT_SEQUENCE_NUMBER", done => {
        const stream = nonEmptyStreamNames[1],
            data = [0, 1].map(() => {
                return { myData: Math.random() * 0x10000000000000 };
            });
        myKinesis
            .putRecords(stream, data, null, myKinesisOptions)
            .then(returnObj =>
                myKinesis.getRecords(
                    stream,
                    "AT_SEQUENCE_NUMBER",
                    returnObj.ShardId,
                    { StartingSequenceNumber: returnObj.SequenceNumber },
                    myKinesisOptions
                )
            )
            .then(records => {
                debug(`records gotten using shard iterator type AT_SEQUENCE_NUMBER: ${JSON.stringify(records)}`);
                expect(records).to.be.an("array");
                expect(records).to.deep.include.members([JSON.stringify(data)]);
                expect(records).to.have.length(data.length);
                done();
            })
            .catch(err => {
                debug(`err: ${err}`);
                done(err);
            });
    });
});
