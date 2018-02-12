var router = require("express").Router();
var streamController = require("../controllers/stream");

router.get("/api/streams", streamController.listStreams);
router.get("/api/stream/:name/summary", streamController.describeStreamSummary);
router.get("/api/stream/:name/records", streamController.getRecords);

module.exports = router;
