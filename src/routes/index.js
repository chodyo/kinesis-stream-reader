var router = require("express").Router();
var streamController = require("../controllers/stream");

router.get("/api/streams", streamController.listStreams);
router.get("/api/stream/:name/summary", streamController.describeStreamSummary);
router.get("/api/stream/:name/records", streamController.readRecords);

router.post("/api/stream/:name/:shardCount", streamController.createStream);
router.put("/api/stream/:name", streamController.putRecords);
router.delete("/api/stream/:name", streamController.deleteStream);

module.exports = router;
