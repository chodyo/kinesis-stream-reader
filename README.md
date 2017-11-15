## Kinesis Stream Reader
This app provides you with a server to display AWS Kinesis stream data as JSON, converting Kinesis stream data to be both human and machine readable. Use this application for:
- manual testing (to ensure that the data you are putting in Kinesis actually gets there the way you want it)
- automated unit tests (to make sure nobody broke your Kinesis message sender tool)

The reader will automatically parse through solo and aggregate messages, showing each Kinesis message as its own JSON object.

## Usage
The server defaults to running on port 4000, and the data will be accessible behind the endpoint `/records`. From that endpoint, additional search parameters can be tacked on from there. For example:
`http://localhost:4000/records?streamname=example-stream&duration=10`
will return all messages sent in the last 10 minutes from the *example-stream* stream.

### Recommended Tools
- JSON in the browser
    - Firefox does this by default :o
	- If you'll be using this service in the browser at all, we recommend you install a browser plugin to format JSON data.

## Installing and Debugging the Server

### 1. Download and install nodejs.

Once you have nodejs and npm, download the kinesis stream reader and install all the dependencies in this project:
```bash
# download the Kinesis Stream Reader project
git clone https://github.com/chody-h/kinesis-stream-reader.git && cd kinesis-stream-reader/app/server
# download project dependencies
npm install
```


### 2. Accessing your kinesis stream
Create a file in the kinesisReader directory (app/server/my_modules/kinesisReader) named `secrets.js` and fill it with the following, entering in your AWS account information where relevant:

```javascript
// app/server/my_modules/kinesisReader/secrets.js

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


### 3. Debugging locally with VS Code.

To enable local debugging in VS Code, add the following *configuration* to your .vscode/launch.json file:

```javascript
{
    "name": "Launch KSR Service",
    "type": "node",
    "request": "launch",
    "program": "${workspaceRoot}/kinesis-stream-reader/app/server/index.js",
    "env": {"DEBUG": "*,-not_this"},
    "stopOnEntry": false
}
```
The "env" attribute is to enable verbose debugging. See the [javascript debug module](https://github.com/visionmedia/debug) for more information.

### 4. Running unit tests locally with VS Code.
To allow VS code to run the unit tests of this project, add the following *configuration* to your .vscode/launch.json file:

```javascript
{
    "name": "Test KSR",
    "type": "node",
    "request": "launch",
    "program": "${workspaceFolder}/kinesis-stream-reader/app/server/node_modules/mocha/bin/_mocha",
    "args": [
        "-u",
        "tdd",
        "--timeout",
        "999999",
        "--colors",
        "${workspaceRoot}/kinesis-stream-reader/app/server/test"
    ],
    "internalConsoleOptions": "openOnSessionStart",
    "stopOnEntry": false
}