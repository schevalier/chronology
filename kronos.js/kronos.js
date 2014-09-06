/*
 When running in a browser, ensure that the domain is added to
 `node.cors_whitelist_domains` in settings.py.
*/
"use strict";
var request;
try {
    request = require("request");
} catch (err) {
    request = null;
}

function KronosClient(options) {
    // `kronosUrl`: The Url of the Kronos server to talk to.
    // `namespace` (optional): Namespace to store events in.
    // `debug` (optional): Log error messages to console?
    var self = this;
    options = options || {};
    var namespace = options.namespace || null;
    var $ = options.jQuery;
    var debug = options.debug || false;
    var noop = function() {};

    self.ID_FIELD = "@id";
    self.TIMESTAMP_FIELD = "@timestamp";
    self.SUCCESS_FIELD = "@success";
    self.ASCENDING_ORDER = "ascending";
    self.DESCENDING_ORDER = "descending";

    var kronosUrl = options.kronosUrl;
    var indexUrl = kronosUrl + "/1.0/index/";
    var putUrl = kronosUrl + "/1.0/events/put";
    var getUrl = kronosUrl + "/1.0/events/get";
    var deleteUrl = kronosUrl + "/1.0/events/delete";
    var streamsUrl = kronosUrl + "/1.0/streams";

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

        var makeRequest = function(url, type, data, onSuccess, onError) {
            onSuccess = onSuccess || noop;
            onError = onError || noop;
            if (typeof request !== "undefined") { //node.js
                request({
                        "url": url,
                        "method": type,
                        "body": data,
                        "headers": {
                            "Content-type": "application/json"
                        }
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
                    "type": type,
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
                xdr(url, type, data, onSuccess, onError);
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

    var parseKronosResponse = function(body, statusCode, callback, errBack) {
        if (statusCode !== 200) {
            errBack(new Error("Error contacting KronosServer, statusCode: " + statusCode));
            return;
        }
        var data;
        try {
            data = JSON.parse(body);
        } catch (err) {
            errBack(new Error("Error parsing JSON data: " + err.message));
            return;
        }
        // Check if there was a server side error?
        if (data[self.SUCCESS_FIELD]) {
            callback(data);
        } else {
            errBack(new Error("Error processing request: " + data));
        }
    };

    self.index = function(callback, errBack) {
        http.get(indexUrl,
            function(body, statusCode) {
                parseKronosResponse(body, statusCode, callback, errBack);
            }, errBack);
    };

    self.put = function(stream, event, namespace, callback, errBack) {
        // `stream`: Stream to put the event in.
        // `event`: The event object.
        // `namespace` (optional): Namespace for the stream.
        event = event || {};
        if (event[self.TIMESTAMP_FIELD] === null) {
            event[self.TIMESTAMP_FIELD] = self.kronosTimeNow();
        }
        var data = {
            namespace: namespace || self.namespace,
            events: {},
        };
        data.events[stream] = [event];
        data = JSON.stringify(data);
        http.post(putUrl, data, function(body, statusCode) {
            parseKronosResponse(body, statusCode, callback, errBack);
        }, errBack);
    };
    return self;
}

(function(exports) {
    module.exports = KronosClient;
})(typeof exports === "undefined" ? this.KronosClient = {} : exports);
