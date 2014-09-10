#!/usr/bin/env node

var fs = require('fs');
var UglifyJS = require('uglify-js');

var result = UglifyJS.minify('kronos.js', {
  mangle: true,
  compress: {
    sequences: true,
    dead_code: true,
    conditionals: true,
    booleans: true,
    unused: true,
    if_return: true,
    join_vars: true,
    drop_console: true
  }
});

fs.writeFileSync('kronos.min.js', result.code);
