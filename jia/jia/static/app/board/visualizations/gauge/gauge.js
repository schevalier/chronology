var module = angular.module('jia.vis.gauge', []);

module.factory('gauge', function () {

  var meta = {
    title: 'gauge',
    readableTitle: 'Gauge',
    template: 'gauge.html',
    css: [
      '/static/app/board/visualizations/gauge/gauge.css'
    ]
  };

  var visualization = function () {
    this.meta = meta;
    this.value = 0;
    this.settings = {
      requiredFields: {
        'Value': '@value'
      },
      optionalFields: {}
    };

    this.setData = function (data, msg) {
      var valueField = this.settings.requiredFields['Value'];
      if (data.events.length == 0) {
        return;
      }
      if (data.events.length > 1) {
        msg.warn("Gauge accepts one value. Only the most recent is shown.");
      }
      this.value = data.events[data.events.length - 1][valueField];
    }
  }

  return {
    meta: meta,
    visualization: visualization
  }
});
