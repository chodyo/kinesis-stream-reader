const expect = require("chai").expect,
    debug = require("debug")("reader-tests");

describe("My kinesis module", function() {
    let myKinesis, myOptions;

    before(function() {
        const kinesisMock = require("kinesalite")({
            path: "./mydb",
            createStreamMs: 50,
            ssl: true
        });
        kinesisMock.listen(4567, function(err) {
            if (err) throw err;
            debug("Kinesalite server started on port 4567");
        });

        myKinesis = require("../scripts/kinesis");

        myOptions = { host: "localhost", port: 4567 };
    });

    describe("with no existing streams", function() {
        it("can ask for stream names", done => {
            myKinesis
                .listStreams(myOptions)
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
            var Writable = require("stream").Writable,
                kinesis = require("kinesis");

            require("https").globalAgent.maxSockets = Infinity;

            var consoleOut = new Writable({ objectMode: true });
            consoleOut._write = function(chunk, encoding, cb) {
                chunk.Data = chunk.Data.slice(0, 10);
                console.dir(chunk);
                cb();
            };

            debug("creating streams");
            // const kinesis = myKinesis.expose();
            newStreamNames.forEach(newStreamName => {
                const newStreamOptions = Object.assign(
                    {},
                    { name: newStreamName },
                    myOptions
                );
                var kinesisStream = kinesis.stream(newStreamOptions);
                kinesisStream.pipe(consoleOut);
            });

            var now = new Date().getTime();
            while (new Date().getTime() < now + 1000) {
                /* do nothing */
            }

            var Readable = require("stream").Readable;

            var readable = new Readable({ objectMode: true });
            readable._read = function() {
                for (var i = 0; i < 100; i++)
                    this.push({
                        PartitionKey: i.toString(),
                        Data: new Buffer("a")
                    });
                this.push(null);
            };

            var kinesisStream = kinesis.stream({
                name: "one",
                writeConcurrency: 5
            });

            readable.pipe(kinesisStream).on("end", function() {
                console.log("done");
            });
        });

        it("can ask for stream names", done => {
            myKinesis
                .listStreams(myOptions)
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
