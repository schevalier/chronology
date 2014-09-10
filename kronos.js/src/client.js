/*
 When running in a browser, ensure that the domain is added to
 `node.cors_whitelist_domains` in settings.py.
*/

"use strict";

var http = require("http");
var Q = require("q");
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
  
  var namespace = options.namespace;

  var makeRequest = function(path, data) {
    var options = {
      path: path,
      hostname: parsedUrl.hostname,
      port: parsedUrl.port,
      protocol: parsedUrl.protocol,
      withCredentials: false // TODO(usmanm): Should we fix at server?
    };
    
    if (data) {
      options.method = "POST";
    } else {
      options.method = "GET";
    }

    return Q.Promise(function(resolve, reject, notify) {
      
      var request = http.request(options, function(response) {
        if (response.statusCode !== 200) {
          reject(new Error("Bad status code: " + response.statusCode));
          return;
        }

        var data = "";

        response.on("data", function(chunk) {
          data += chunk;
        });

        response.on("end", function () {
          try {
            resolve(JSON.parse(data.trim()));
          } catch(error) {
            reject(error);
          }
        });
      });

      request.on("error", function(error) {
        reject(new Error("Fatal error (lol) " + error.message));
      });

      if (data) {
        request.write(JSON.stringify(data));
      }

      request.end();
    });
  };

  var makeStreamingRequest = function(path, data) {
    var options = {
      method: "POST",
      path: path,
      hostname: parsedUrl.hostname,
      port: parsedUrl.port,
      protocol: parsedUrl.protocol,
      withCredentials: false // TODO(usmanm): Should we fix at server?
    };

    var defer = Q.defer();
    var responseComplete = false;
    var items = [];
    var iterFunction;

    var flushItems = function() {
      if (!iterFunction) {
        return;
      }
      while (items.length) {
        iterFunction(items.shift());
      }
      if (responseComplete) {
        defer.resolve();
      }
    };

    var pushItem = function(item) {
      items.push(item);
      flushItems();
    };

    var each = function(func) {
      if (iterFunction) {
        throw new Error("`each` function already registered.");
      }

      iterFunction = func;

      if (responseComplete) {
        flushItems();
      }

      return this;
    };

    defer.promise.each = each.bind(defer.promise);

    var request = http.request(options, function(response) {
      if (response.statusCode !== 200) {
        defer.reject(new Error("Bad status code: " + response.statusCode));
        return;
      }

      var data = "";
      var i, splits;

      var handleStreaming = function(offset) {
        if (!data.length) {
          return;
        }
        splits = data.split("\n");
        for (i = 0; i < splits.length - offset; i++) {
          var item = splits[i].trim();
          if (!item.length) {
            break;
          }
          /// XXX(usmanm): Does this preserve order when notifying?
          if (item.charAt(0) === "{") {
            try {
              item = JSON.parse(item);
            } catch(error) {
              defer.reject(error);
              request.abort();
            }
          }
          pushItem(item);
        }
      };

      response.on("data", function(chunk) {
        data += chunk;
        handleStreaming(1); // Don't consume the last chunk.
        data = splits[splits.length - 1];
      });
      
      response.on("end", function () {
        handleStreaming();
        responseComplete = true;
        flushItems();
      });
    });

    request.on("error", function(error) {
      defer.reject(new Error("Fatal error (lol) " + error.message));
    });

    if (data) {
      request.write(JSON.stringify(data));
    }

    request.end();

    return defer.promise;
  };

  self.index = function() {
    return makeRequest(indexPath, null);
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
    return makeRequest(putPath, requestData);
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
    return makeStreamingRequest(getPath, requestData);
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
    return makeRequest(deletePath, requestData);
  };

  self.getStreams = function(namespace) {
    var requestData = {"namespace": namespace || self.namespace};
    return makeStreamingRequest(getStreamsPath, requestData);
  };

  self.inferSchema = function(stream, namespace) {
    var requestData = {"stream": stream,
                       "namespace": namespace || self.namespace};
    return makeRequest(inferSchemaPath, requestData);
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
