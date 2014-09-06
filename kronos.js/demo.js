/*
# kronos.js

## Introduction
The contents of this file can be found in `demo.js` and are compiled
into `README.md`, so you can consume the readme while running the
javascript program with node.js (`make demo`) to understand how it works.

The kronos.js package contains support for both the browser and a node.js server, either will work seamlessly with the client. For simplicity, we use node.js for this demo.
*/
"use strict";
var util = require("util");
var KronosClient = require("./kronos.js");

var pprint = function(object) {
    return util.inspect(object, false, null);
};

/*
Create a Kronos client with the URL of a running server. Optionally
provide a `namespace` to explicitly work with events in a particular namespace.
*/
var kc = new KronosClient({
    "kronosUrl": "http://127.0.0.1:8151",
    "namespace": "kronos",
});
var startTime = kc.kronosTimeNow();


/*
## KronosRequests
  Each request method (detailed below) accepts an optional `callback` and `errBack` function.
## Index Request
The calling `kc.index()` will return information about the running Kronos server.
*/
kc.index(function(kronosResponse) {
    console.log("KronosIndex: " + pprint(kronosResponse));
});
/*
## Inserting data
Insert data with the `kc.put()` command. The argument is a stream to
insert the data into. You can provide a single event which holds an
arbitrary dictionary of JSON encodeable data.
*/
var stream = "yourproduct.website.pageviews";

// make an event to log
var event = {
    "source": "http://test.com",
    "browser": {
        "name": "Chrome",
        "version": 26,
    },
    "pages": ["page1.html", "page2.html"],
};
kc.put(stream, event, null, function(kronosResponse) {
    console.log("KronosPut: " + pprint(kronosResponse));
}); // use the client's namespace

/*
### Optionally add a timestamp
By default, each event will be timestamped on the client. If you add
a `kc.TIMESTAMP_FIELD` argument, you can specify the time at which each
event ocurred.
*/
event[kc.TIMESTAMP_FIELD] = kc.timeToKronosTime(new Date(1980, 1, 6, 0, 0, 0, 0));
kc.put(stream, event, null, function(kronosResponse) {
    console.log("KronosPut: " + pprint(kronosResponse));
});
