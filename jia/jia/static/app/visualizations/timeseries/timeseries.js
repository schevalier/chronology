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
    ]

  };

  var visualization = function () {

    this.meta = meta;
    this.data = [];
    this.chart = {
      data: {
        xs: {},
        columns: []
      },
      legend: {},
      point: {
        show: true 
      },
      tooltip: {
        grouped: false
      },
      axis: {
        x: {
          type: 'timeseries',
          tick: {
            format: '%d-%m-%Y %H:%M:%S %Z',
            count: 20 
          }
        }
      }
    };

    this.settings = {
      requiredFields: {
        'X-Axis': '@time',
        'Y-Axis': '@value'
      },
      optionalFields: {
        'Series': '@series'
      }
    };

    this.showAll = function () {
      
    };

    this.setData = function (data, msg) {
      // `data` should contain an `events` property, which is a list
      // of Kronos-like events.  An event has at least two fields `@time`
      // (Kronos time: 100s of nanoseconds since the epoch), and
      // `@value`, a floating point value.  An optional `@series`
      // attribute will split the event stream into different
      // groups/series.  All events in the same `@series` will be
      // plotted on their own line.
      this.data = data;

      var timeField = this.settings.requiredFields['X-Axis'];
      var valueField = this.settings.requiredFields['Y-Axis'];
      var groupField = this.settings.optionalFields['Series'];

      var series = _.groupBy(data.events, function(event) {
        return event[groupField] || 'series';
      });
      this.series = Object.keys(series);

      if (_.size(series) > 1) {
        this.chart.legend['show'] = true;
      }
      else {
        this.chart.legend['show'] = false;
      }
      
      var chartData = this.chart.data;
      _.each(series, function (events, key) {
        chartData.xs[key] = 'x' + key;
        
        var x = ['x' + key];
        var y = [key];

        _.each(events, function(value, index) {
          var d = new Date(0);
          d.setUTCSeconds(value[timeField] * 1e-7);
          x.push(d);
          y.push(value[valueField]);
        });

        chartData.columns.push(x);
        chartData.columns.push(y);
      });
    }
  }

  return {
    meta: meta,
    visualization: visualization
  }
});
