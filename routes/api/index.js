var router = require("express").Router();
var recordsRoutes = require("./records");

router.use("/records", recordsRoutes);

module.exports = router;