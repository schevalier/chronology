angular.module('angular-c3', [])
  .directive('c3', function($compile) {
    return {
      restrict: 'EA',
      scope: {
        chart: '='
      },
      link: function(scope, element, attrs) {
        function update () {
          element = angular.element(element);
          element.empty();
          var container = $compile('<div></div>')(scope);
          element.append(container);
          scope.chart['bindto'] = container[0];
          scope.chart['instance'] = c3.generate(scope.chart);
        }
        
        scope.$watch('chart', function (val, prev) {
          if (!angular.equals(val, prev)) {
            update();
          }
        });

        update();
      }
    };
  });
