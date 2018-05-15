const chai = require("chai"),
    chaiAsPromised = require("chai-as-promised"),
    expect = chai.expect,
    debug = require("debug")("reader-tests"),
    kinesalite = require("kinesalite"),
    myKinesis = require("../scripts/kinesis");

chai.use(chaiAsPromised);

const kinesaliteServer = kinesalite({
        path: "./mydb",
        ssl: true,
        createStreamMs: 0,
        deleteStreamMs: 0
    }),
    myKinesisOptions = {
        host: "localhost",
        port: 4567,
        region: "us-east-1",
        credentials: { accessKeyId: "dummy", secretAccessKey: "dummy" }
    };

function getRandomData() {
    return Math.random() * 0x10000000000000;
}

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
        return new Promise((resolve, reject) => {
            Promise.all(
                nonEmptyStreamNames.map(streamName =>
                    myKinesis
                        .deleteStream(streamName, myKinesisOptions)
                        .then(deleteStreamResponse => {
                            debug(`stream ${streamName} deleted with response: ${deleteStreamResponse}`);
                        })
                        .catch(err => {
                            debug(`could not delete stream ${streamName}: ${err}`);
                            reject(err);
                        })
                )
            ).then(() => {
                debug("deleted all streams");
                resolve();
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
            data = { myRecord: getRandomData() };
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

    it("fails gracefully when attempting to put a record in a non-existent stream", () => {
        const stream = getRandomData().toString(),
            data = { myRecord: getRandomData() };
        const noStreamPromise = myKinesis.putRecord(stream, data, null, myKinesisOptions);
        return expect(noStreamPromise).to.eventually.be.rejectedWith(
            `Stream ${stream} under account 000000000000 not found.`
        );
    });

    it("can get a record using shard iterator type AT_SEQUENCE_NUMBER", done => {
        const stream = nonEmptyStreamNames[0],
            data = { myRecord: getRandomData() };
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
                expect(records).to.have.length(1);
                expect(records).to.deep.equal([data]);
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
                return { myRecord: getRandomData() };
            });
        myKinesis
            .putRecords(stream, data, null, myKinesisOptions)
            .then(returnObj => {
                debug(`putRecords return obj: ${JSON.stringify(returnObj)}`);
                expect(returnObj.FailedRecordCount).to.equal(0);
                return myKinesis.getRecords(
                    stream,
                    "AT_SEQUENCE_NUMBER",
                    returnObj.Records[0].ShardId,
                    { StartingSequenceNumber: returnObj.Records[0].SequenceNumber },
                    myKinesisOptions
                );
            })
            .then(records => {
                debug(`records gotten using shard iterator type AT_SEQUENCE_NUMBER: ${JSON.stringify(records)}`);
                expect(records).to.be.an("array");
                expect(records).to.have.length(data.length);
                expect(records).to.deep.equal(data);
                done();
            })
            .catch(err => {
                debug(`err: ${err}`);
                done(err);
            });
    });

    it("fails gracefully when attempting to get a record from a non-existent stream", () => {
        const stream = getRandomData().toString();
        const noStreamPromise = myKinesis.getRecords(
            stream,
            "AT_SEQUENCE_NUMBER",
            null,
            { StartingSequenceNumber: "0" },
            myKinesisOptions
        );
        return expect(noStreamPromise).to.eventually.be.rejectedWith(
            `Shard shardId-000000000000 in stream ${stream} under account 000000000000 does not exist`
        );
    });

    it("can delete a stream", done => {
        // choose a stream to delete and clean it up from the list. this will
        // help the `after` function not try to delete a non-existant stream
        const streamIndex = 2,
            stream = nonEmptyStreamNames[streamIndex];
        nonEmptyStreamNames.splice(streamIndex, 1);
        myKinesis
            .deleteStream(stream, myKinesisOptions)
            .then(deleteStreamResponse => {
                debug(`deleteStreamResponse: ${deleteStreamResponse}`);
                return myKinesis.listStreams(myKinesisOptions);
            })
            .then(streams => {
                debug(`streams: ${streams}`);
                expect(streams).to.be.an("array");
                expect(streams).to.deep.equal(nonEmptyStreamNames);
                done();
            })
            .catch(err => {
                debug(`error: ${err}`);
                done(err);
            });
    });

    it("fails gracefully when attempting to delete a stream that doesn't exist", () => {
        const stream = getRandomData().toString();
        const noStreamPromise = myKinesis.deleteStream(stream, myKinesisOptions);
        return expect(noStreamPromise).to.eventually.be.rejectedWith(
            `Stream ${stream} under account 000000000000 not found.`
        );
    });
});
