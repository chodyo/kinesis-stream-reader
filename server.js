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
    debug("Listening on port: " + PORT, process.env.NODE_ENV);
});
