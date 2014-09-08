/*
 When running in a browser, ensure that the domain is added to
 `node.cors_whitelist_domains` in settings.py.
*/
"use strict";
var request;
try {
    request = require("request");
} catch (err) {}

/*
 * @param options {Object}
 *    @param kronosUrl {String} Url for running Kronos server
 *    @param namespace {String} (optional): Namespace to store events in.
 *    @param jQuery {Object} (optional): use jQuery to make AJAX requests.
 */
function KronosClient(options) {
    if (typeof options.kronosUrl === "undefined") {
        throw new Error("A kronosUrl must be provided");
    }
    var kronosUrl = options.kronosUrl;
    var namespace = options.namespace || null;
    var $ = options.jQuery;

    var indexUrl = kronosUrl + "/1.0/index/";
    var putUrl = kronosUrl + "/1.0/events/put";
    var getUrl = kronosUrl + "/1.0/events/get";
    var deleteUrl = kronosUrl + "/1.0/events/delete";
    var streamsUrl = kronosUrl + "/1.0/streams";

    var self = this;
    var noop = function() {};
    var HTTP_OK = 200;

    self.ID_FIELD = "@id";
    self.TIMESTAMP_FIELD = "@timestamp";
    self.SUCCESS_FIELD = "@success";
    self.ASCENDING_ORDER = "ascending";
    self.DESCENDING_ORDER = "descending";

    /*
     * Get the current time as KronosTime
     */
    self.kronosTimeNow = function() {
        return self.timeToKronosTime(new Date());
    };

    /*
     * Convert a Date type object to a KronosTime object
     *
     * @param date {Date}
     */
    self.timeToKronosTime = function(date) {
        return date.getTime() * 1e4;
    };

    /*
     * Code from: https://gist.github.com/eriwen/2794392#file-cors-js
     *
     * Make a X-Domain request to url and callback.
     *
     * @param url {String}
     * @param method {String} HTTP verb ("GET", "POST", "DELETE", etc.)
     * @param data {String} request body
     * @param callback {Function} to callback on completion
     * @param errBack {Function} to callback on error
     */
    var xdr = function(url, method, data, callback, errBack) {
        var req;

        if (typeof XMLHttpRequest !== "undefined") {
            req = new XMLHttpRequest();

            if ("withCredentials" in req) {
                req.open(method, url, true);
                req.onerror = errBack;
                req.onreadystatechange = function() {
                    if (req.readyState === 4) {
                        if (req.status >= 200 && req.status < 400) {
                            callback(req.responseText, req.status);
                        } else {
                            errBack(new Error("Response returned with non-OK status."));
                        }
                    }
                };
                req.send(data);
            }
        } else if (typeof XDomainRequest !== "undefined") {
            req = new XDomainRequest();
            req.open(method, url);
            req.onerror = errBack;
            req.onload = function() {
                callback(req.responseText, req.status);
            };
            req.send(data);
        } else {
            errBack(new Error("CORS not supported."));
        }
    };

    var http = (function() {
        /*
         * Wrap the node.js `request` module, jQuery `$.ajax` and `xdr` http functionality into a single interface.
         * Client calls use whichever library is available to execute the client logic.
         *
         * @param url {String}
         * @param type{String} HTTP verb ("GET", "POST", "DELETE", etc.)
         * @param data {String} request body
         * @param onSuccess {Function} to callback on completion
         * @param onError {Function} to callback on error
         */
        var makeRequest = function(url, method, data, onSuccess, onError) {
            onSuccess = onSuccess || noop;
            onError = onError || noop;
            data = JSON.stringify(data);
            if (typeof request !== "undefined") { //node.js
                request({
                        "url": url,
                        "method": method,
                        "body": data,
                        "headers": {
                            "Content-type": "application/json"
                        },
                    },
                    function(error, response, body) {
                        if (error) {
                            onError(error);
                        } else {
                            onSuccess(body, response.statusCode);
                        }
                    });
            } else if (typeof $ !== "undefined") { //jQuery
                $.ajax({
                    "type": method,
                    "url": url,
                    "data": data,
                    "dataType": "json",
                    "success": function(data, statusText, xhr) {
                        onSuccess(data, xhr.status);
                    },
                    "error": function(jqXHR, textStatus, errorThrown) {
                        onError(errorThrown);
                    }
                });
            } else { // xdr AJAX request
                xdr(url, method, data, onSuccess, onError);
            }
        };

        var get = function(url, onSuccess, onError) {
            makeRequest(url, "GET", null, onSuccess, onError);
        };

        var post = function(url, data, onSuccess, onError) {
            makeRequest(url, "POST", data, onSuccess, onError);
        };
        return {
            "get": get,
            "post": post
        };
    }());

    /*
     * Parse the body of a HTTP Response into json.
     * Returns an object with any errors that occured during parsing or the
     * parsed body
     * @param body {String}
     */
    var parseJSON = function(body) {
        var err, responseData;
        try {
            body = body.trim().replace("\r", "").split("\n");
            responseData = [];
            var object;
            for (var i = 0; i < body.length; i++) {
                object = body[i];
                if (object !== "") {
                    responseData.push(JSON.parse(object));
                }
            }
        } catch (e) {
            err = new Error("Error parsing JSON data: " + e.message);
        }
        return {
            "err": err,
            "data": responseData,
        };
    };

    /*
     * Returns whether or not the given `statusCode` is equal to `HTTP_OK`
     * @param statusCode {int}
     */
    var httpSuccess = function(statusCode) {
        return statusCode === HTTP_OK;
    };

    /*
     * Returns a new `Error` object with a message and `statusCode`
     * @param statusCode {int}
     */
    var getServerError = function(statusCode) {
        return new Error("Error contacting KronosServer, statusCode: " + statusCode);
    };

    /*
     * Parse a response from a http request into json and send the results to the `callback` function.
     * `errBack` can be called if the http `statusCode` is not OK, no JSON results are returned or the `SUCCESS_FIELD`
     * is not truthy.
     *
     * @param body {String}
     * @param statusCode{int}
     * @param callback {Function} to callback on completion
     * @param errBack {Function} to callback on error
     */
    var parseKronosResponse = function(body, statusCode, callback, errBack) {
        if (!httpSuccess(statusCode)) {
            errBack(getServerError(statusCode));
            return;
        }
        var responseData = parseJSON(body);
        var data = responseData.data[0];
        if (responseData.err) {
            errBack(responseData.err);
        }
        // Check if there was a server side error?
        else if (data[self.SUCCESS_FIELD]) {
            callback(data);
        } else {
            errBack(new Error("Error processing request: " + responseData.data));
        }
    };

    /*
     * Parse a string or Date into kronos time.
     * @param time {String || Date || int}
     */
    var parseTime = function(time) {
        if (typeof time === "string") {
            time = Date.parse(time);
        }
        if (time instanceof Date) {
            time = self.timeToKronosTime(time);
        }
        return time;
    };

    /*
     * Helper function to set `namespace` in the requestDict.
     * Tries to use options and falls back to the client level namespace, if present.
     *
     * @param options {Object}
     * @param requestDict {Object}
     */
    var setNamespace = function(options, requestDict) {
        var namespace = options.namespace || self.namespace;
        if (namespace) {
            requestDict.namespace = namespace;
        }
    };

    /*
     * Helper function to either `startId` or `startTime` in the requestDict.
     *
     * @param options {Object}
     * @param startTime {KronosTime}
     * @param requestDict {Object}
     */
    var setStart = function(options, startTime, requestDict) {
        if (options.startId) {
            requestDict.start_id = options.startId;
        } else {
            requestDict.start_time = startTime;
        }
    };

    self.index = function(callback, errBack) {
        http.get(indexUrl,
            function(body, statusCode) {
                parseKronosResponse(body, statusCode, callback, errBack);
            }, errBack);
    };

    /*
     * @param stream {String} Stream to put the event in.
     * @param event {Object} The event object.
     * @param namespace {String} (optional): Namespace for the stream.
     * @param callback {Function} to callback on completion
     * @param errBack {Function} to callback on error
     */
    self.put = function(stream, event, namespace, callback, errBack) {
        event = event || {};
        if (!event[self.TIMESTAMP_FIELD]) {
            event[self.TIMESTAMP_FIELD] = self.kronosTimeNow();
        }
        var data = {
            namespace: namespace || self.namespace,
            events: {},
        };
        data.events[stream] = [event];
        http.post(putUrl, data, function(body, statusCode) {
            parseKronosResponse(body, statusCode, callback, errBack);
        }, errBack);
    };

    /*
     * Queries a stream with name `stream` for all events between `startTime` and
     * `endTime` (both inclusive).  An optional `startId` allows the client to
     * restart from a failure, specifying the last ID they read; all events that
     * happened after that ID will be returned. An optional `limit` limits the
     * maximum number of events returned.  An optional `order` requests results in
     * `ASCENDING_ORDER` or `DESCENDING_ORDER`.
     */
    self.get = function(stream, startTime, endTime, options, callback, errBack) {
        options = options || {};
        startTime = parseTime(startTime);
        endTime = parseTime(endTime);
        var order = options.order || self.ASCENDING_ORDER;
        var requestDict = {
            "stream": stream,
            "end_time": endTime,
            "order": order,
        };

        if (options.startId) {
            requestDict.start_id = options.startId;
        } else {
            requestDict.start_time = startTime;
        }

        if (options.limit) {
            requestDict.limit = options.limit;
        }

        setNamespace(options, requestDict);

        var errorCount = 0;
        var maxErrors = 10;

        var retry = function(errorCount, requestDict, callback, errBack) {
            callback = callback || noop;
            errBack = errBack || noop;
            var err;

            // TODO(jblum): stream response
            http.post(getUrl, requestDict, function(body, statusCode) {
                var responseData;
                if (!httpSuccess(statusCode)) {
                    errorCount++;
                    err = getServerError(statusCode);
                } else {
                    responseData = parseJSON(body);
                    if (responseData.err) {
                        errorCount++;
                        err = responseData.err;
                    }
                }
                if (errorCount >= maxErrors) {
                    errBack(err);
                } else if (err) {
                    retry(errorCount, requestDict, callback, errBack);
                } else {
                    callback(responseData.data);
                }
            }, function(err) {
                errorCount++;
                if (errorCount >= maxErrors) {
                    errBack(err);
                } else if (err) {
                    retry(errorCount, requestDict, callback, errBack);
                }
            });
        };
        retry(errorCount, requestDict, callback, errBack);
    };

    /*
     * Delete events in the stream with name `stream` that occurred between
     * `start_time` and `end_time` (both inclusive).  An optional `startId` allows
     * the client to delete events starting from after an ID rather than starting
     * at a timestamp.
     */
    self.delete = function(stream, startTime, endTime, options, callback, errBack) {
        options = options || {};
        startTime = parseTime(startTime);
        endTime = parseTime(endTime);

        var requestDict = {
            "stream": stream,
            "end_time": endTime,
        };

        setStart(options, startTime, requestDict);
        setNamespace(options, requestDict);

        http.post(deleteUrl, requestDict,
            function(body, statusCode) {
                parseKronosResponse(body, statusCode, callback, errBack);
            }, errBack);
    };

    /*
     * Queries the Kronos server and fetches a list of streams available to be read.
     */
    self.get_streams = function(options, callback, errBack) {
        options = options || {};
        var requestDict = {};
        setNamespace(options, requestDict);

        http.post(streamsUrl, requestDict, function(body, statusCode) {
            if (!httpSuccess(statusCode)) {
                errBack(getServerError(statusCode));
            } else {
                var responseData = parseJSON(body);
                if (responseData.err) {
                    errBack(responseData.err);
                } else {
                    callback(responseData.data);
                }
            }
        }, errBack);
    };
    return self;
}

if (typeof module !== "undefined") {
    module.exports = KronosClient;
}
