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
});
