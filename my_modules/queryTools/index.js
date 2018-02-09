/**
 * @typedef     validatedParams
 * @property    {boolean}   badRequest
 *              Indicates if the parameters given are invalid or in the
 *              incorrect form.
 * @property    {string[]}  missingRequiredParams
 *              All parameters that should have been in the request but weren't.
 * @property    {string[]}  invalidParams
 *              All parameters that were given and not recognized.
 */

////////////////////////////////////////////////////////////////////////////////
//-------------------------- query parameters ---------------------------------
//\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

module.exports = class QueryParamValidator {
    constructor(allowed, required) {
        this.allowed = new Set(allowed);
        this.required = required;
    }

    /**
     * Ensures all required query params are in the URL and all query params are valid.
     * @param   {Object}    request
     *          An object containing a property for each query string parameter in
     *          the route. If there is no query string, it is the empty object, {}.
     * @returns {validatedParams}
     *          Whether or not query params were valid and reasons why the query
     *          params were invalid, if applicable.
     */
    validateParams(request) {
        // initialize return value
        const details = {};
        details.badRequest = false;
        details.missingRequiredParams = [];
        details.invalidParams = [];

        // find all required params that are not in the request
        const required = this.required.filter(required => {
            return request[required] === undefined;
        });
        // if any required attributes were not listed as parameters, that's an error
        if (required.length > 0) {
            details.badRequest = true;
            details.missingRequiredParams = required;
        }

        // iterate over the requestParams object, getting every attribute contained
        // therein. this is probably optional, but will help fix typos.
        var invalidQueryParams = Object.keys(request).filter(param => {
            return !this.allowed.has(param);
        });

        // if any attributes were not specified as allowed params, that's an error
        if (invalidQueryParams.length > 0) {
            details.badRequest = true;
            details.invalidParams = invalidQueryParams;
        }

        return details;
    }
};
