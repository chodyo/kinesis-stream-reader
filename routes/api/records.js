var router = require("express").Router();
var recordsController = require("../../controllers/records");

router.get("/", recordsController.getRecords);

module.exports = router;