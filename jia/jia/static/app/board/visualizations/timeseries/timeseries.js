var module = angular.module('jia.vis.timeseries', ['angular-rickshaw']);

module.factory('timeseries', function () {

  var meta = {
    title: 'timeseries',
    readableTitle: 'Time Series',
    template: 'timeseries.html',

    css: [
      "//cdnjs.cloudflare.com/ajax/libs/rickshaw/1.4.6/rickshaw.min.css",
    ],

    js: [
      "//cdnjs.cloudflare.com/ajax/libs/d3/3.4.1/d3.min.js",
      "//cdnjs.cloudflare.com/ajax/libs/rickshaw/1.4.6/rickshaw.min.js",
      "//ngyewch.github.io/angular-rickshaw/rickshaw.js"
    ],

  };

  var visualization = function () {

    this.meta = meta;
    this.series = [{name: 'series', data: [{x: 0, y: 0}]}];

    this.settings = {
      requiredFields: {
        'X-Axis': '@time',
        'Y-Axis': '@value'
      },
      optionalFields: {
        'Series': '@series'
      }
    };

    this.timeseriesOptions = {
      renderer: 'line',
      width: parseInt($('.panel').width() * .73),
      interpolation: 'linear'
    };

    this.timeseriesFeatures = {
      palette: 'spectrum14',
      xAxis: {},
      yAxis: {},
      hover: {},
    };
    
    this.setData = function (data, msg) {
      // `data` should contain an `events` property, which is a list
      // of Kronos-like events.  An event has at least two fields `@time`
      // (Kronos time: 100s of nanoseconds since the epoch), and
      // `@value`, a floating point value.  An optional `@series`
      // attribute will split the event stream into different
      // groups/series.  All events in the same `@series` will be
      // plotted on their own line.

      // TODO(marcua): do a better job of resizing the plot given the
      // legend size.
      this.timeseriesOptions.width = parseInt($('.panel').width() * .73);

      var timeField = this.settings.requiredFields['X-Axis'];
      var valueField = this.settings.requiredFields['Y-Axis'];
      var groupField = this.settings.optionalFields['Series'];

      var compare = function (a, b) {
        return a[timeField] - b[timeField];
      }

      data.events.sort(compare);

      var series = _.groupBy(data.events, function(event) {
        return event[groupField] || 'series';
      });
      delete this.timeseriesFeatures.legend;

      if (_.size(series) > 0) {
        series = _.map(series, function(events, seriesName) {
          return {name: seriesName, data: _.map(events, function(event) {
            return {x: event[timeField] * 1e-7, y: event[valueField]};
          })}
        });
        if (_.size(series) > 1) {
          this.timeseriesFeatures.legend = {toggle: true, highlight: true};
        }
      } else {
        series = [{name: 'series', data: [{x: 0, y: 0}]}];
      }

      this.series = series;
    }
  }

  return {
    meta: meta,
    visualization: visualization
  }
});
