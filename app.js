var http = require('http');
var deagg = require('aws-kinesis-agg/kpl-deagg')
var exceptionCount = 0;
var exData = '';
var shardIterator = '';
var url = require('url');
var common = require('./common');

var kinesis = require('./secrets').getKinesis();
var streamName;
var response;
// global AggregatedRecord object which will hold the protocol buffer model
var AggregatedRecord = common.loadBuilder();


// -------------- NODE SERVER -------------- 
var app = http.createServer(function (req, res) {
	var params = processRequest(req);
	response = res;
	setResponse(params);
});
app.listen(4000);
console.log('Listening on Port 4000....');

	console.log('YES!!!!!!!!! New Request received: ' + req.url);

	var queryObject = url.parse(req.url, true).query;
	var passedTimeFrame = 0;
	if (typeof queryObject.duration !== 'undefined' && queryObject)
		passedTimeFrame = queryObject.duration;
	var myStartTime = Math.floor(Date.now() / 1000) - passedTimeFrame * 60;

	if (typeof queryObject.streamname !== 'undefined' && queryObject)
		streamName = queryObject.streamname;
	else
		streamName = 'evolve-test_cti_events';

	return {
		ShardId: '0', /* required */
		ShardIteratorType: 'AT_TIMESTAMP', /* required */
		StreamName: streamName, /* required */
		Timestamp: myStartTime
	};
}


// -------------- AWS SECTION -------------- 

var setResponse = function (params) {
	try {
		kinesis.getShardIterator(params, afterShardIterator);
	}
	catch (e) {
		console.log(e, e.stack);
		response.setHeader('Content-type', 'text/plain');
		response.write("Invalid Streamname: " + streamName + "\nOR I've no clue whats going on.");
		response.end();
	}
}

var afterShardIterator = function (err, data) {
	if (!err) {
		if (data.ShardIterator != '') {
			params = {
				ShardIterator: data.ShardIterator, /* required */
				Limit: 100
			};
			kinesis.getRecords(params, handleKinesisRecords);
		} else {
			response.setHeader('Content-type', 'text/plain');
			response.write("No Shard Iterator received");
			response.end();
		}
	}
	else {
		console.log(err, err.stack);
		response.setHeader('Content-type', 'text/plain');
		response.write("Invalid Streamname: " + streamName + "\nOR I've no clue whats going on.");
		response.end();
	}
}

var handleKinesisRecords = function (err, data) {
	var separator = ",\n";
	var jsonResponse = '[';
	if (!err) {
		var allRecords = data.Records;
		for (var i = 0; i < allRecords.length; ++i) {
			aggregateRecord = allRecords[i];
			// jsonResponse += agg.deaggregateSync(aggregateRecord.kinesis, getRecordAsJson);
			var deaggregatedList = deaggregate(aggregateRecord, false, getRecordAsJson);
			jsonResponse += deaggregatedList.join(separator);
			if (i != allRecords.length - 1) jsonResponse += separator;
		}
		jsonResponse += ']';
	}
	//console.log('Ganesh Your Final data is :  '+jsonResponse);
	if (jsonResponse != '[]') {
		response.writeHead(200, { "Content-Type": "application/json" });
		response.write(jsonResponse);
	}
	else {
		response.writeHead(200, { "Content-Type": "text/plain" });
		response.write("Stream " + streamName + " exists, but there were no records found in it.");
	}
	response.end();
}

// based off of https://github.com/awslabs/kinesis-aggregation/blob/master/node/node_modules/aws-kinesis-agg/kpl-deagg.js
var deaggregate = function (kinesisRecord, computeChecksums, perRecordCallback, afterRecordCallback) {
	"use strict";
	/* jshint -W069 */// suppress warnings about dot notation (use of
	// underscores in protobuf model)
	//
	// we receive the record data as a base64 encoded string
	var recordBuffer = new Buffer(kinesisRecord.Data, 'base64');
	var records = [];

	// first 4 bytes are the kpl assigned magic number
	// https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
	if (recordBuffer.slice(0, 4).toString('hex') === kplConfig[useKplVersion].magicNumber) {
		try {
			if (!AggregatedRecord) {
				AggregatedRecord = common.loadBuilder();
			}

			// decode the protobuf binary from byte offset 4 to length-16 (last
			// 16 are checksum)
			var protobufMessage = AggregatedRecord.decode(recordBuffer.slice(4,
				recordBuffer.length - 16));

			// extract the kinesis record checksum
			var recordChecksum = recordBuffer.slice(recordBuffer.length - 16,
				recordBuffer.length).toString('base64');

			if (computeChecksums === true) {
				// compute a checksum from the serialised protobuf message
				var md5 = crypto.createHash('md5');
				md5.update(recordBuffer.slice(4, recordBuffer.length - 16));
				var calculatedChecksum = md5.digest('base64');

				// validate that the checksum is correct for the transmitted
				// data
				if (calculatedChecksum !== recordChecksum) {
					if (debug) {
						console.log("Record Checksum: " + recordChecksum);
						console.log("Calculated Checksum: "
							+ calculatedChecksum);
					}
					throw new Error("Invalid record checksum");
				}
			} else {
				if (debug) {
					console
						.log("WARN: Record Checksum Verification turned off");
				}
			}

			if (debug) {
				console.log("Found " + protobufMessage.records.length
					+ " KPL Encoded Messages");
			}

			// iterate over each User Record in order
			for (var i = 0; i < protobufMessage.records.length; i++) {
				try {
					var item = protobufMessage.records[i];

					// emit the per-record callback with the extracted partition
					// keys and sequence information
					var record = perRecordCallback(
						null,
						{
							partitionKey: protobufMessage["partition_key_table"][item["partition_key_index"]],
							explicitPartitionKey: protobufMessage["explicit_hash_key_table"][item["explicit_hash_key_index"]],
							sequenceNumber: kinesisRecord.sequenceNumber,
							subSequenceNumber: i,
							data: item.data.toString('base64')
						});
					records.push(record);
				} catch (e) {
				}
			}
		} catch (e) {
		}
	} else {
		// not a KPL encoded message - no biggie - emit the record with
		// the same interface as if it was. Customers can differentiate KPL
		// user records vs plain Kinesis Records on the basis of the
		// sub-sequence number
		if (debug) {
			console
				.log("WARN: Non KPL Aggregated Message Processed for DeAggregation: "
				+ kinesisRecord.partitionKey
				+ "-"
				+ kinesisRecord.sequenceNumber);
		}
		var record = perRecordCallback(null, {
			partitionKey: kinesisRecord.PartitionKey,
			// explicitPartitionKey : kinesisRecord.explicitPartitionKey,
			sequenceNumber: kinesisRecord.SequenceNumber,
			data: kinesisRecord.Data
		});
		records.push(record);
	}
	return records;
};

var getRecordAsJson = function (err, singleRecord) {
	if (!err) {
		// foreach
		var entry = new Buffer(singleRecord.data, 'base64').toString();
		var entryAsJson = entry;
		// entryAsJson += ',{"break": "---------------------------------------------------End Of Record---------------------------------------------------"}';
		return entryAsJson;
	}
};