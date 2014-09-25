"use strict";

var assert = require("assert");

/*
 * Parse a string or Date into Kronos time.
 * @param time {String, Date, int}
 */
var toKronosTime = module.exports.toKronosTime = function(time) {
  if (typeof time === "string") {
    time = Date.parse(time);
  }
  if (time instanceof Date) {
    time = time.getTime() * 1e4;
  }
  assert.ok(typeof time === "number");
  time = parseInt(time);
  return time;
};

/*
 * Get the current time as Kronos time.
 */
module.exports.kronosTimeNow = function() {
  return toKronosTime(new Date());
};
