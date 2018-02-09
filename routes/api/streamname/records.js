var router = require("express").Router({ mergeParams: true });
var recordsController = require("../../../controllers/records");

router.get("/", recordsController.getRecords);

module.exports = router;
