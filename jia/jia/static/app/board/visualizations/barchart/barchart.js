var module = angular.module('jia.vis.barchart', ['angular-c3']);

module.factory('barchart', function () {

  var meta = {
    title: 'barchart',
    readableTitle: 'Bar Chart',
    template: 'barchart.html',
    css: [
      '/static/app/board/visualizations/barchart/barchart.css'
    ],
  };

  var visualization = function () {
    this.meta = meta;
    this.data = [];
    this.chart = {
      data: {
        columns: []
      }
    };
    this.settings = {
      requiredFields: {
        'X-Axis': '@label',
        'Y-Axis': '@value'
      },
      optionalFields: {
        'Stack by': '@group'
      }
    };

    this.setData = function (data, msg) {
      this.data = data;
      var error = false;
      var labelField = this.settings.requiredFields['X-Axis'];
      var valueField = this.settings.requiredFields['Y-Axis'];
      var groupField = this.settings.optionalFields['Stack by'];

      // In the future, this may be an option in the query builder
      var stacked = true;

      var groups = [];
      var series = _.groupBy(data.events, function(event) {
        return event[groupField] || '';
      });

      var c3legend = {};

      var categories = [];
      if (_.size(series) > 0) {
        var i = 0;
        series = _.map(series, function(events, seriesName) {
          var cats = [];

          // Extract the values from the events and build a list
          var data = _.map(events, function(event) {
            // Build a list of categories (x axis) and check for duplicates
            if (i == 0 && _.contains(cats, event[labelField])) {
              msg.warn('Duplicate label "' + event[labelField] + '"');
            }
            cats.push(event[labelField]);
            return event[valueField];
          });

          // On first iteration, save the categories list
          if (i == 0) {
            categories = cats;
          }
          // On all other iterations, check to make sure the categories
          // are consistent
          else {
            if (!_.isEqual(categories, cats)) {
              msg.error("All groups must have the same labels");
              error = true;
              return [];
            }
          }

          i++;

          // C3 expects the group name to be the first item in the array
          // followed by all the data points
          if (typeof events[0][groupField] != 'undefined') {
            data.unshift(events[0][groupField]);
            c3legend['show'] = true;
          }
          else {
            data.unshift(valueField);
            c3legend['show'] = false;
          }

          if (stacked) {
            groups.push(events[0][groupField]);
          }

          return data;
        });
      } else {
        series = [];
      }

      var cols = [];

      // If there is an error, do not display the half calculated data
      if(!error) {
        cols = series;
      }

      var c3data = {
        columns: cols,
        type: 'bar',
        groups: [groups]
      };

      var c3axis = {
        x: {
          type: 'category',
          categories: categories
        }
      };

      this.chart = {
        data: c3data,
        axis: c3axis,
        legend: c3legend
      };
    }
  }

  return {
    meta: meta,
    visualization: visualization
  }
});
