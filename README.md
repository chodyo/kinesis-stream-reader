#You must create a file in the root directory (with app.js) named

`secrets.js`


Fill it with the following, entering in the data where relevant:

--------------------------------- `secrets.js` ---------------------------------
```
var AWS = require('aws-sdk');

// NEVER EVER EVER EVER UPLOAD THIS TO A REPOSITORY!!!!!!!!!!
module.exports.getKinesis = function() {
    return new AWS.Kinesis({
        apiVersion: 'xxxxxxxxxx',
        accessKeyId: 'xxxxxxxxxxxxxxxxxxxx',
        secretAccessKey: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        region: 'xxxxxxxxx'
    });
};
```