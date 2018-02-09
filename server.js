// Kinesis Reader back end.
// -/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/-/

// Require our dependencies
var express = require("express");

// Set up our port to be either the host's designated port, or 3000
var PORT = process.env.PORT || 3000;

// Instantiate our Express App
var app = express();

// Designate our public folder as a static directory
app.use(express.static("public"));

// Require our routes
var routes = require("./routes");

// Have every request go through our route middleware
app.use(routes);

// Listen on the port
app.listen(PORT, function() {
  console.log("Listening on port: " + PORT);
});