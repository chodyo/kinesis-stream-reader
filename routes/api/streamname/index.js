var router = require("express").Router({ mergeParams: true });
var recordsRoutes = require("./records");

router.use("/:duration", recordsRoutes);

module.exports = router;
