/*
 When running in a browser, ensure that the domain is added to
 `node.cors_whitelist_domains` in settings.py.
*/

"use strict";

var http = require("./http");
var time = require("./time");
var url = require("url");
var VERSION = require("./version");

var ID_FIELD = "@id";
var LIBRARY_FIELD = "@library";
var TIMESTAMP_FIELD = "@time";
var SUCCESS_FIELD = "@success";

var indexPath = "/1.0/index";
var putPath = "/1.0/events/put";
var getPath = "/1.0/events/get";
var deletePath = "/1.0/events/delete";
var getStreamsPath = "/1.0/streams";
var inferSchemaPath = "/1.0/streams/infer_schema";

/*
 * @param options {Object} - A set of key/value pairs that configure the
 * Kronos client instance. All settings except `host` are optional.
 *    @param url {String} URL for running Kronos server.
 *    @param namespace {String} (optional): Namespace to store/fetch events.
 */
var KronosClient = function(options) {
  var self = this;

  if (!options.url) {
    throw new Error("Must provide a non-null `url`.");
  }

  var parsedUrl = url.parse(options.url);
  if (!parsedUrl.hostname) {
    throw new Error("Invalid `url` specified.");
  }

  var request = new http.Client(parsedUrl).request;
  var namespace = options.namespace;

  self.index = function() {
    return request(indexPath);
  };

  self.put = function(stream, event, namespace) {
    if (!event) {
      event = {};
    }
    if (!event[TIMESTAMP_FIELD]) {
      event[TIMESTAMP_FIELD] = time.kronosTimeNow();
    }
    event[LIBRARY_FIELD] = {
      "name": "kronos.js",
      "version": VERSION
    };
    var events = {};
    events[stream] = [event];
    var requestData = {
      "namespace": namespace || self.namespace,
      "events": events
    };
    return request(putPath, requestData);
  };

  self.get = function(stream, startTime, endTime, namespace, options) {
    options = options || {};
    var requestData = {
      "namespace": namespace || self.namespace,
      "stream": stream,
      "end_time": endTime,
      "order": options.order || KronosClient.Order.ASCENDING
    };
    /* jshint camelcase: false */
    if (options.startId) {
      requestData.start_id = options.startId;
    } else {
      requestData.start_time = startTime;
    }
    if (options.limit) {
      requestData.limit = options.limit;
    }
    // TODO(usmanm): Add retry.
    return request(getPath, requestData, {stream: true});
  };

  self.delete = function(stream, startTime, endTime, namespace, options) {
    var requestData = {
      "namespace": namespace || self.namespace,
      "stream": stream,
      "end_time": endTime
    };
    /* jshint camelcase: false */
    if ((options || {}).startId) {
      requestData.start_id = options.startId;
    } else {
      requestData.start_time = startTime;
    }
    return request(deletePath, requestData);
  };

  self.getStreams = function(namespace) {
    var requestData = {"namespace": namespace || self.namespace};
    return request(getStreamsPath, requestData, {stream: true});
  };

  self.inferSchema = function(stream, namespace) {
    var requestData = {"stream": stream,
                       "namespace": namespace || self.namespace};
    return request(inferSchemaPath, requestData);
  };
};

KronosClient.Order = KronosClient.prototype.Order = {
  ASCENDING: "ascending",
  DESCENDING: "descending"
};

module.exports = {
  "KronosClient": KronosClient,
  "VERSION": VERSION,
  "toKronosTime": time.toKronosTime,
  "kronosTimeNow": time.kronosTimeNow,
  "ID_FIELD": ID_FIELD,
  "TIMESTAMP_FIELD": TIMESTAMP_FIELD
};
