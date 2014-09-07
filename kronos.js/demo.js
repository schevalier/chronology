/*
# kronos.js

## Introduction
The contents of this file can be found in `demo.js` and are compiled
into `README.md`, so you can consume the readme while running the
javascript program with node.js (`make demo`) to understand how it works.

The kronos.js package contains support for both the browser and a node.js server, either will work seamlessly with the client. For simplicity, we use node.js for this demo.

If you would like to see an in browser demo, simply open the webpage `demo.html` in your browser and open a debugger to see the console messages.
*/
"use strict";
try { // demo.html
    var util = require("util");
    var KronosClient = require("./kronos.js");

    var pprint = function(object) {
        return util.inspect(object, false, null);
    };

} catch (e) {
    pprint = function(object) {
        return object;
    };
}

var logResponse = function(kronosResponse, msg) {
    for (var i = 0; i < kronosResponse.length; i++) {
        console.log(msg + ": " + pprint(kronosResponse[i]));
    }
};

var sleepTime = 1000; // ms

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
/*
## Retrieving data
Retrieving data requires a `stream` name, a `startTime`, and an `endTime`.
Note that an `kc.ID_FIELD` and `kc.TIMESTAMP_FIELD` field are
attached to each event. The `kc.ID_FIELD` is a UUID1-style identifier
with its time bits derived from the timestamp. This allows event IDs
to be roughly sortable by the time that they happened while providing
a deterministic tiebreaker when two events happened at the same time.
*/
setTimeout(function() {
    kc.get(stream, startTime, kc.kronosTimeNow(), null, function(kronosResponse) {
        logResponse(kronosResponse, "Recieved event");
    });
}, sleepTime);
/*
### Event order
By default, events are returned in ascending order of their
`kc.ID_FIELD`. Pass in the optional argument `kc.DESCENDING_ORDER` argument to
change this behavior to be in descending order of `kc.ID_FIELD`.
*/
setTimeout(function() {
    var options = {
        "order": kc.DESCENDING_ORDER
    };
    kc.get(stream, startTime, kc.kronosTimeNow(), options, function(kronosResponse) {
        logResponse(kronosResponse, "Reverse event");
    });
}, sleepTime);

/*
### Limiting events
If you only want to retrieve a limited number of events, use the
`limit` argument.
*/
setTimeout(function() {
    var options = {
        "limit": 1,
    };
    kc.get(stream, startTime, kc.kronosTimeNow(), options, function(kronosResponse) {
        logResponse(kronosResponse, "Limited event");
    });
}, sleepTime);

/*
   ## Getting a list of streams
   To see all streams available in this namespace, use `kc.get_streams`.
*/

setTimeout(function() {
    kc.get_streams(null, function(kronosResponse) {
        logResponse(kronosResponse, "Found stream");
    });
}, sleepTime);

/*
   ## Deleting data
   Sometimes, we make an oopsie and need to delete some events.  The
   `kc.delete` function takes similar arguments for the start and end
   timestamps to delete.

   Note: The most common Kronos use cases are for write-mostly systems
   with high-throughput reads.  As such, you can imagine that most
   backends will not be delete-optimized.  There's nothing in the Kronos
   API that inherently makes deletes not performant, but we imagine some
   backends will make tradeoffs to optimize their write and read paths at
   the expense of fast deletes.
*/

setTimeout(function() {
    kc.delete(stream, startTime, kc.kronosTimeNow(), null, function(kronosResponse) {
        var numDeleted = kronosResponse[stream].memory.num_deleted;
        console.log("Deleted " + numDeleted + " events");
    });
}, sleepTime);
