#!/usr/bin/env node
"use strict";

var spawn = require("child_process").spawn;
var request = require("request");

var startTestServer = function() {
  console.log("Firing up test kronos server...");
  var kronosProc = spawn(
    "run_kronos.py",
    ["--config", "tests/conf/kronos_settings.py", "--port", "9191"],
    {cwd: __dirname});
  return kronosProc;
};

var pingTestServer = function(onSuccess, onError, numRetries) {
  if (!numRetries) {
    numRetries = 0;
  }

  var retry = function(error) {
    if (numRetries >= 10) {
      onError(error);
    } else {
      setTimeout(function() {
        pingTestServer(onSuccess, onError, numRetries);
      }, 50);
    }
  };

  numRetries++;
  request({url: "http://localhost:9191/1.0/index", timeout: 50},
          function(error, response) {
            if (error || response.statusCode !== 200) {
              retry(error);
            } else {
              onSuccess();
            }
          });
};

var runTests = function(kronosProc) {
  console.log("Running tests...");
  var mochaProc = spawn(
    "node_modules/.bin/mocha",
    ["--reporter", "list", "tests/test_*.js"],
    {cwd: __dirname, stdio: "inherit"});
  mochaProc.on("close", function(code) {
    kronosProc.kill();
  });
};

var kronosProc = startTestServer();
pingTestServer(
  function() {
    runTests(kronosProc);
  },
  function(error) {
    console.log("ERROR: Failed to start test kronos server!");
    console.log(error.message);
  }
);
