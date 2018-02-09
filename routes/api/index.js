var router = require("express").Router();
var streamnameRoutes = require("./streamname");

router.use("/:streamname", streamnameRoutes);

module.exports = router;
