// Kinesis Reader back end.
// -/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/

// Require our dependencies
const express = require("express");
const debug = require("debug")("reader-server");
const bodyParser = require("body-parser");

// Set up our port to be either the host's designated port, or 3000
const PORT = process.env.PORT || 3000;

// Instantiate our Express App
const app = express();

// Designate our public folder as a static directory
app.use(express.static("public"));

// Use bodyParser in our app
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

// Require our routes
const routes = require("./routes");

// Have every request go through our route middleware
app.use(routes);

// Kinesalite server
const kinesalite = require("kinesalite");
const kinesialiteServer = kinesalite({
    path: "./mydb",
    createStreamMs: 50,
    ssl: true
});
kinesialiteServer.listen(4567, function(err) {
    if (err) throw err;
    debug("Kinesalite started on port 4567");
});

// Listen on the port
app.listen(PORT, function() {
    debug("Listening on port: " + PORT);
});






var Writable = require('stream').Writable,
    kinesis = require('kinesis')

require('https').globalAgent.maxSockets = Infinity

var consoleOut = new Writable({objectMode: true})
consoleOut._write = function(chunk, encoding, cb) {
  chunk.Data = chunk.Data.slice(0, 10)
  console.dir(chunk)
  cb()
}

var kinesisStream = kinesis.stream({name: 'cody-test-1', oldest: true, host: "localhost", port: 4567})

kinesisStream.pipe(consoleOut)



// var Readable = require('stream').Readable

// var readable = new Readable({objectMode: true})
// readable._read = function() {
//   for (var i = 0; i < 100; i++)
//     this.push({PartitionKey: i.toString(), Data: new Buffer('a')})
//   this.push(null)
// }

// var kinesisStream = kinesis.stream({name: 'cody-test-1', writeConcurrency: 5, host: "localhost", port: 4567})

// readable.pipe(kinesisStream).on('end', function() { console.log('done') })