"use strict";
var assert = require("assert");
var KronosClient = require("../kronos.js");

var options = {
    "kronosUrl": "http://127.0.0.1:8151",
    "namespace": "kronos",
    "debug": true,
};

function setupClient() {
    return new KronosClient(options);
}

describe("KronosClient", function() {
        describe("index", function() {
            it("Returns Kronos server metadata", function(done) {
                var kc = setupClient();
                kc.index(function(data) {
                    done();
                }, done);
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
                kc.put(stream, event, namespace, function() {
                    kc.put(stream, null, null, function() {
                        done();
                    }, done);
                }, done);
            });

        });

        describe("putAndGet", function() {
            it("Puts and gets events from the Kronos server.", function(done) {
                var kc = setupClient();
                var stream = "test_put_and_get";
                var startTime = kc.kronosTimeNow();

                var eventCount = 5;

                var assertGet = function(completed, expectedCount, done) {
                    if (completed === expectedCount) {
                        kc.get(stream, startTime, kc.kronosTimeNow(), null, function(kronosResponse) {
                            assert.equal(expectedCount, kronosResponse.length, "get returns everything that was put");
                            var limit = 2;
                            var options = {
                                "limit": limit,
                                "order": kc.ASCENDING_ORDER,
                            };
                            var endTime = kc.kronosTimeNow();
                            var count = 0;
                            kc.get(stream, startTime, endTime, options, function(kronosResponse) {
                                assert.equal(limit, kronosResponse.length, "Limit parameter");
                                var lastId = "";
                                var curId;
                                for (var i = 0; i < kronosResponse.length; i++) {
                                    curId = kronosResponse[i][kc.ID_FIELD];

                                    if (lastId !== "") {
                                        assert(lastId <= curId, "Order parameter");
                                    }
                                    lastId = curId;
                                }
                                options = {
                                    "startId": lastId,
                                };
                                kc.get(stream, startTime, endTime, options, function(kronosResponse) {
                                    assert.equal(eventCount - limit, kronosResponse.length, "LastId parameter");
                                    done();
                                }, done);
                            }, done);
                        }, done);
                    }
                };

                var event, id;
                var completed = 0;
                for (var i = 0; i < eventCount; i++) {
                    id = "test_event-" + i;
                    event = {
                        id: i,
                    };
                    kc.put(stream, event, null, function(kronosResponse) {
                        completed++;
                        assertGet(completed, eventCount, done);
                    }, done);
                }


            });
        });


        describe("delete", function() {
            it("Deletes events for the given range", function(done) {
                var kc = setupClient();
                var event, id;
                var completed = 0;
                var stream = "test_delete";
                var putCount1 = 2;
                var startTime = kc.kronosTimeNow();
                for (var i = 0; i < putCount1; i++) {
                    id = "test_event-" + i;
                    event = {
                        id: i,
                    };
                    kc.put(stream, event, null, function(kronosResponse) {
                        completed++;
                        if (completed === putCount1) {
                            completed = 0;
                            var midTime = kc.kronosTimeNow();
                            var putCount2 = 3;
                            var event, id;
                            for (var i = 0; i < putCount2; i++) {
                                id = "test_event-" + i;
                                event = {
                                    id: i,
                                };
                                kc.put(stream, event, null, function(kronosResponse) {
                                    completed++;
                                    if (completed === putCount2) {
                                        var endTime = kc.kronosTimeNow();
                                        kc.get(stream, startTime, endTime, null, function(kronosResponse) {
                                            assert.equal(putCount1 + putCount2, kronosResponse.length, "");
                                        }, done);
                                        kc.delete(stream, startTime, midTime, null, function(kronosResponse) {
                                            assert.equal(putCount1, kronosResponse[stream].memory.num_deleted, "Delete function");
                                        });
                                        kc.get(stream, startTime, endTime, null, function(kronosResponse) {
                                            assert.equal(putCount2, kronosResponse.length, "");
                                            done();
                                        }, done);
                                    }
                                }, done);
                            }
                        }

                    }, done);
                }

            });
            describe("get_streams", function() {
                it("Gets available streams from the Kronos server", function(done) {
                    var kc = setupClient();
                    var streamCount = 5;
                    var completed = 0;
                    var streamName = "stream-";
                    var streamId;
                    for (var i = 0; i < streamCount; i++) {
                        streamId = streamName + i;
                        kc.put(streamId, null, null, function(kronosResponse) {
                            completed++;
                            if (completed === streamCount) {
                                kc.get_streams(null, function(kronosResponse) {
                                    var count = 0;
                                    for (var i = 0; i < kronosResponse.length; i++) {
                                        if (kronosResponse[i].indexOf(streamName) !== -1) {
                                            count++;
                                        }
                                    }
                                    assert(streamCount, count, "Retrieve all streams");
                                    done();
                                }, done);
                            }
                        }, done);
                    }

                });
            });
        });
});
