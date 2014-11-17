/* global describe, it */

"use strict";

var assert = require("assert");
var kronos = require("../index.js");
var Q = require("q");

var options = {
  "url": "http://localhost:9191",
  "namespace": "kronos"
};

function setupClient() {
  return new kronos.KronosClient(options);
}

function putMany(kc, stream, eventCount, callback) {
  var event, id;
  var completed = 0;
  var handler = function(response) {
    completed++;
    if (completed === eventCount) {
      callback();
    }
  };
  for (var i = 0; i < eventCount; i++) {
    id = "test_event-" + i;
    event = {
      "id": i,
    };
    kc.put(stream, event).then(handler);
  }
}

describe("KronosClient", function() {
  describe("index", function() {
    it("Index funciton returns Kronos server metadata.", function(done) {
      var kc = setupClient();
      kc.index().then(function(data) {
        done();
      });
    });
  });

  describe("put", function() {
    it("Puts an event on the Kronos server", function(done) {
      var kc = setupClient();
      var stream = "test_put";
      var event = {
        "test_event": 1,
      };
      var namespace = "kronos";
      kc.put(stream, event, namespace).then(function(response) {
        done();
      });
    });
  });

  describe("putAndGet", function() {
    it("Puts and gets events from the Kronos server.", function(done) {
      var kc = setupClient();
      var stream = "test_put_and_get";
      var startTime = kronos.kronosTimeNow();

      var numEvents = 5;
      putMany(kc, stream, numEvents, function() {
        var endTime = kronos.kronosTimeNow();
        var promises = [];

        var count1 = 0;
        promises.push(
          kc.get(stream, startTime, endTime).each(function(event) {
            count1 += 1;
          }).then(function() {
            assert.equal(
              count1,
              numEvents,
              "Call to get did not return all the objects that were put."
            );
          })
        );

        var count2 = 0;
        var startId;
        promises.push(
          kc.get(stream, startTime, endTime, null, {"limit": 1}).each(
            function(event) {
              count2 += 1;
              startId = event[kronos.ID_FIELD];
            }
          ).then(function() {
            assert.equal(
              count2,
              1,
              "Call to get did not return all the objects that were put."
            );
          })
        );

        var events = [];
        promises.push(
          kc.get(stream, startTime, endTime, null,
                 {"order": kc.Order.DESCENDING}).each(
            function(event) {
              events.push(event);
            }
          ).then(function() {
            var time = Infinity;
            events.forEach(function(event) {
              assert.ok(event[kronos.TIMESTAMP_FIELD] <= time,
                       "Incorrect order.");
              time = event[kronos.TIMESTAMP_FIELD];
            });
            assert.equal(
              events.length,
              numEvents,
              "Call to get did not return all the objects that were put."
            );
          })
        );

        Q.allSettled(promises).then(function() {
          var i = 0;
          promises.push(
            kc.get(stream, startTime, endTime, null, {"startId": startId}).each(
              function(event) {
                if (i == 0) {
                  assert.equal(event[kronos.ID_FIELD], startId,
                              "First event was not equal to start_id.");
                }
                i++;
              }
            ).then(function() {
              assert.equal(
                i,
                numEvents,
                "Call to get did not return all the objects that were put."
              );
              done();
            })
          );
        });
      });
    });
  });

  describe("delete", function() {
    it("Deletes events for the given range", function(done) {
      var kc = setupClient();
      var stream = "test_delete";
      var numEvents = 2;
      var startTime = kronos.kronosTimeNow();
      putMany(kc, stream, numEvents, function() {
        var endTime = kronos.kronosTimeNow();
        kc.delete(stream, startTime, endTime).then(function(response) {
          Object.keys(response[stream]).forEach(function(backend) {
            /* jshint camelcase: false */
            assert.equal(numEvents,
                         response[stream][backend].num_deleted,
                         "Delete function returned the wrong number of items.");
          });
          done();
        });
      });
    });
  });

  describe("getStreams", function() {
    it("Gets available streams from the Kronos server", function(done) {
      var kc = setupClient();
      var numStreams = 5;
      var streamName = "stream-";

      var promises = [];
      for (var i = 0; i < numStreams; i++) {
        promises.push(kc.put(streamName + i, {}));
      }

      Q.all(promises).then(function() {
        var streams = [];
        kc.getStreams().each(function(stream) {
          streams.push(stream);
        }).then(function() {
          assert.ok(streams.length >= numStreams,
                    "getStreams failed.");
          for (var i = 0; i < numStreams; i++) {
            assert.ok(streams.indexOf(streamName + i) !== -1,
                      "getStreams failed.");
          }
          done();
        });
      });
    });
  });

  describe("inferSchema", function() {
    it("Checks if infer_schema endpoint is working.", function(done) {
      var kc = setupClient();
      var stream = "test_infer_schema";
      var event = {
        "test_event": 1,
      };
      kc.put(stream, event).then(function() {
        kc.inferSchema(stream).then(function(response) {
          assert.equal(response.stream, stream);
          assert.ok(response.schema);
          done();
        });
      });
    });
  });
});
