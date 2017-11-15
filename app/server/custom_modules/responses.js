
module.exports = {
    ok: ok,
    invalidRequest: invalidRequest
};

/**
 * Container object for all responses the KSR will return.
 */
////////////////////////////////////////////////////////////////////////////////
//----------------------------- responses -------------------------------------
//\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\


/**
 * Will set responses as desired AND END!!!!
 * @param   {Object}                    res
 *          The same response object created in the NodeJS route handler.
 * @param   {*}                         message
 *          Whatever description you want to send to the user in the body of
 *          the response. No body will be sent if no message is defined.
 * @param   {Object.<string, string>}   headers
 *          Key-Value pairs of HTTP headers that will be sent with the
 *          response.
 */
function _base(res, message, headers) {
    if (message) {
        // this.setHeader('Content-type', 'text/plain');
        res.write(JSON.stringify(message));
    }
    if (Object.keys(headers).length !== 0) {
        Object.keys(headers).forEach(function (key) {
            res.setHeader(key, headers[key]);
        });
    }
    res.end();
}

/**
 * Will set the response to 400, the body to 'message', and any headers given.
 * Does not validate the headers or message first.
 * @param   {Object}                    res
 *          The same response object created in the NodeJS route handler.
 * @param   {*}                         [message]
 *          Whatever description you want to send to the user in the body of
 *          the response. No body will be sent if no message is defined.
 * @param   {Object.<string, string>}   [headers]
 *          Key-Value pairs of HTTP headers that will be sent with the
 *          response.
 */
function invalidRequest(res, message = null, headers = {}) {
    res.statusCode = 400;
    _base(res, message, headers);
}

/**
 * Will set the response to 200, the body to 'message', and any headers given.
 * Does not validate the headers or message first.
 * @param   {Object}                    res
 *          The same response object created in the NodeJS route handler.
 * @param   {*}                         [message]
 *          Whatever description you want to send to the user in the body of
 *          the response. No body will be sent if no message is defined.
 * @param   {Object.<string, string>}   [headers]
 *          Key-Value pairs of HTTP headers that will be sent with the
 *          response.
 */
function ok(res, message = null, headers = {}) {
    res.statusCode = 200;
    _base(res, message, headers);
}
