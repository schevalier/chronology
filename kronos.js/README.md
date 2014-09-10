# Kronos.js

## Introduction
The contents of this file can be found in `demo.js` and are compiled
into `README.md`, so you can consume the readme while running the
JavaScript program with [Node.js](http://nodejs.org/) (`make demo`) to
understand how it works.

The kronos.js library provides support for browsers and Node.js. To use it in a
browser, simply drop a `<script>` tag into your HTML linking to `kronos.js` or
`kronos.min.js`. To use it in Node.js, `require` the `index.js` module. We will
publish kronos.js on [npm](https://www.npmjs.org/) soon.

If you would like to see an in browser demo, simply open the webpage `demo.html`
in your browser.
```javascript

"use strict";

if (typeof module !== "undefined" && module.exports) { // Node.js
  var util = require("util");
  var kronos = require("./index.js");
  var pprint = function(object) {
    return util.inspect(object, false, null);
  };
  var log = console.log;
} else { // Browser
  var pprint = function(object) {
    return JSON.stringify(object, null, " ");
  };
  var log = function(string) {
    var div = document.getElementById("log");
    var p = document.createElement("p");
    p.innerHTML = string;
    div.appendChild(p);
  };
}
```
Create a Kronos client with the URL of a running server. Optionally
provide a `namespace` to explicitly work with events in a particular namespace.
```javascript
var kronosClient = new kronos.KronosClient({
  "url": "http://localhost:8150",
  "namespace": "kronos"
});
```
## Making Requests
Each request method (detailed below) returns a
[Q.Promise](https://github.com/kriskowal/q/wiki/API-Reference#promise-methods)
object.

## Index Request
The calling `kronosClient.index()` will return information about the running
Kronos server.
```javascript
kronosClient.index().then(function(response) {
  log("KronosIndex: " + pprint(response));
});
```
## Inserting Events
Insert events with the `kronosClient.put()` command. The first argument is a
stream name to insert the events into. You can provide a single event which
holds an arbitrary dictionary of JSON encodeable key/values.
```javascript
var stream = "yourproduct.website.pageviews";
var event_ = {
  "source": "http://test.com",
  "browser": {
    "name": "Chrome",
    "version": 26,
  },
  "pages": ["page1.html", "page2.html"],
};
kronosClient.put(stream, event_).then(function(response) {
  log("KronosPut: " + pprint(response));
});
```
### Optionally Add A Timestamp
By default, each event will be timestamped on the client. If you add
a `kronos.TIMESTAMP_FIELD` property, you can specify the time at which each
event ocurred.
```javascript
event_[kronos.TIMESTAMP_FIELD] = kronos.toKronosTime(new Date(1980, 1, 6));
kronosClient.put(stream, event_).then(function(response) {
  log("KronosPut: " + pprint(response));
});
```
## Retrieving Events
Retrieving events requires a `stream` name, a `startTime`, and an `endTime`.
The `namespace` argument is optional and the `options` argument is explained
below.



Note that an `kronos.ID_FIELD` and `kronos.TIMESTAMP_FIELD` field are always
attached to each event. The `kronos.ID_FIELD` is a UUID1-style identifier
with its time bits derived from the timestamp. This allows event IDs
to be roughly sortable by the time that they happened while providing
a deterministic tiebreaker when two events happened at the same time.
```javascript
setTimeout(function() {
  var events = [];
  // The `Q.Promise` object returned by `kronosClient.get` has an additional
  // `each` method which takes a function as the only argument. This
  // function is called (in-order) for each event returned by the server.
  // In this example, we're simply storing all the events returned into an
  // Array. The `Q.Promise` object returned is resolved when all events have
  // been consumed.
  // TODO(usmanm): Some way to abort iteration?
  kronosClient.get(stream, 0, kronos.kronosTimeNow()).each(
    function(event) {
      events.push(event);
    }
  ).then(function() {
    log("KronosGet: " + pprint(events));
  });
}, 750);
```
### Event Order
By default, events are returned in ascending order of their
`kronosClient.ID_FIELD`. Pass in the optional `order` argument as
`kronosClient.Order.DESCENDING` to change this behavior to be in
descending order of `kronos.ID_FIELD`.
```javascript
setTimeout(function() {
  var options = {
    "order": kronosClient.Order.DESCENDING
  };
  var events = [];
  kronosClient.get(stream, 0, kronos.kronosTimeNow(), null, options).each(
    function(event) {
      events.push(event);
    }
  ).then(function() {
    log("KronosGet {order: " + kronosClient.Order.DESCENDING + "}: " +
        pprint(events));
  });
}, 750);
```
### Limiting Events
If you only want to retrieve a limited number of events, use the
optional `limit` argument.
```javascript
setTimeout(function() {
  var options = {
    "limit": 1,
  };
  var events = [];
  kronosClient.get(stream, 0, kronos.kronosTimeNow(), null, options).each(
    function(event) {
      events.push(event);
    }
  ).then(function() {
    log("KronosGet {limit: 1}: " + pprint(events));
  });
}, 750);
```
## Getting List Of All Streams
To see all streams available in this namespace, use `kronosClient.getStreams`.
The behavior of this method is identical to `kronosClient.get` except this
yields stream names (Strings) rather than events (Objects).
```javascript
setTimeout(function() {
  var streams = [];
  kronosClient.getStreams().each(
    function(stream) {
      streams.push(stream);
    }
  ).then(function() {
    log("KronosGetStreams: " + streams);
  });
}, 750);
```
## Deleting Events
Sometimes, we make an oopsie and need to delete some events.  The
`kronosClient.delete` function accepts the same arguments as `kronosClient.get`.

Note: The most common Kronos use cases are for write-mostly systems
with high-throughput reads.  As such, you can imagine that most
backends will not be delete-optimized.  There's nothing in the Kronos
API that inherently makes deletes not performant, but we imagine some
backends will make tradeoffs to optimize their write and read paths at
the expense of fast deletes.
```javascript
setTimeout(function() {
  kronosClient.delete(stream, 0, kronos.kronosTimeNow()).then(
    function(response) {
      log("KronosDelete: " + pprint(response));
    }
  );
}, 1500);
```
