"use strict";

var http = require("http");
var Q = require("q");

var makeRequest = function(parsedUrl, path, data) {
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

var makeStreamingRequest = function(parsedUrl, path, data) {
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

module.exports.Client = function(parsedUrl) {
  var self = this;

  self.request = function(path, data, options) {
    options = options || {};
    if (options.stream) {
      return makeStreamingRequest(parsedUrl, path, data);
    }
    return makeRequest(parsedUrl, path, data);
  };
};
