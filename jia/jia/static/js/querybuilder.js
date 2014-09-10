/*
 * Components of the Visual Query Builder (VQB)
 * --------------------------------------------
 *
 *
 * CPF (constant/property/function):
 *
 * cpf = {
 *   cpf_type: <string>,
 *   constant_value: <string>,
 *   property_name: <string>,
 *   function_name: <string>,
 *   function_args: [<cpf>, <cpf>, ...]
 * }
 *
 *
 * Structure of a query object:
 * 
 * query = {
 *   stream: <string>,
 *   steps: [<step>, <step>, ...]
 * }
 *
 * step = {
 *   operation: <operation>
 * }
 *
 * TODO(derek): In the future, query should be a tree of operations, and the
 * stream name should be incorporated into a kstream access method operator.
 *
 * Sample operation:
 * 
 * operation = {
 *   operator: 'aggregate',
 *   operands: {
 *     'aggregates': [<aggregation>, ...],
 *     'groups': [<group>, ...],
 *   }
 * }
 * 
 * aggregation = {
 *   agg_type: <string>,
 *   agg_on: <cpf>,
 *   alias: <string>
 * }
 * 
 * group = {
 *   field: <cpf>,
 *   alias: <string>
 * }
 * 
 */


var qb = angular.module('jia.querybuilder', []);

function findObjectInListBasedOnKey(list, keyName, keyVal) {
  for (var i = 0; i < list.length; i++) {
    if (list[i][keyName] == keyVal) {
      return list[i];
    }
  }
}

qb.directive('querybuilder', function ($http, $compile) {
  var controller = ['$scope', function($scope) {
    $scope.nextStep = {};
    $scope.query = $scope.panel.data_source.query;

    $scope.$watch('nextStep.operation', function (newVal, oldVal) {
      if (newVal && !_.isEmpty(newVal)) {
        var newStep = angular.copy($scope.nextStep);
        $scope.query.steps.push(newStep);
        $scope.nextStep = {};
      }
    });

    $scope.delete = function (step) {
      var index = $scope.query.steps.indexOf(step);
      if (index > -1) {
        $scope.query.steps.splice(index, 1);
      }
    };
  }];
  
  return {
    restrict: 'E',
    templateUrl: '/static/partials/querybuilder.html',
    controller: controller,
    scope: {
      panel: '='
    }
  };
});

qb.directive('step', function ($http, $compile) {
  var linker = function (scope, element, attrs) {
    scope.$watch(function () {
      return scope.step.operation;
    }, function (newVal, oldVal) {
      if (!scope.newop && typeof newVal != 'undefined') {
        $http.get(['static', 'partials', 'operators',
                   scope.step.operation.operator + '.html'].join('/'))
          .success(function(data, status, headers, config) {
            $(element).find('div.args').html(data);
            $compile(element.contents())(scope);
          });
      }
    });
  }

  var controller = ['$scope', function($scope) {
    $scope.operations = [
      {
        name: 'Transform',
        operator: 'transform',
        operands: {}
      },
      {
        name: 'Filter',
        operator: 'filter',
        operands: {}
      },
      {
        name: 'Order by',
        operator: 'orderby',
        operands: {
          'fields': [{}]
        }
      },
      {
        name: 'Limit',
        operator: 'limit',
        operands: {}
      },
      {
        name: 'Aggregate',
        operator: 'aggregate',
        operands: {
          'aggregates': [{}],
          'groups': [{}]
        }
      }
    ];

    $scope.addOperand = function (operands) {
      operands.push({});
    };
    $scope.removeOperand = function (idx, operands) {
      operands.splice(idx, 1);
    }
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/step.html',
    controller: controller,
    link: linker,
    scope: {
      step: '=',
      newop: '='
    }
  };
});


qb.directive('cpf', function ($http, $compile) {
  var controller = ['$scope', function ($scope) {
    $scope.functions = [
      {
        name: 'Ceiling',
        value: 'Ceiling',
        options: [
          {name: 'Property', type: 'property'},
          {name: 'Base', type: 'constant'},
          {name: 'Offset', type: 'constant'}
        ]
      },
      {
        name: 'Floor',
        value: 'Floor',
        options: [
          {name: 'Property', type: 'property'},
          {name: 'Base', type: 'constant'},
          {name: 'Offset', type: 'constant'}
        ]
      },
      {
        name: 'Date Truncate',
        value: 'DateTrunc',
        options: [
          {name: 'Property', type: 'property'},
          {
            name: 'Time scale',
            type: 'constant',
            choices: [
              'second',
              'minute',
              'hour',
              'day',
              'week',
              'month',
              'year',
            ]
          }
        ]
      },
      {
        name: 'Date Part',
        value: 'DatePart',
        options: [
          {name: 'Property', type: 'property'},
          {
            name: 'Time scale',
            type: 'constant',
            choices: [
              'second',
              'minute',
              'hour',
              'weekday',
              'day',
              'month'
            ]
          }
        ]
      },
      {
        name: 'Lowercase',
        value: 'Lowercase',
        options: [
          {name: 'Property', type: 'property'}
        ]
      },
      {
        name: 'Uppercase',
        value: 'Uppercase',
        options: [
          {name: 'Property', type: 'property'}
        ]
      },
      {
        name: 'Random Integer',
        value: 'RandInt',
        options: [
          {name: 'Low', type: 'constant'},
          {name: 'High', type: 'constant'}
        ]
      }
      /*
       * TODO(derek): Missing functions
       *
       * or make an HTTP endpoint for determining this info
       */
    ];
    $scope.func = $scope.functions[0];

    $scope.types = [
      {name: 'Property', type: 'property'},
      {name: 'Constant', type: 'constant'},
      {name: 'Function', type: 'function'}
    ];
    $scope.type = $scope.types[0];
    $scope.args = [];
        
    if (!$scope.arg || $scope.arg.property_only) {
      $scope.arg = {};
    }
    else if ($scope.arg.cpf_type) {
      $scope.type = findObjectInListBasedOnKey($scope.types, 'type',
                                               $scope.arg.cpf_type);
      $scope.func = findObjectInListBasedOnKey($scope.functions, 'value',
                                               $scope.arg.function_name);
      _.each($scope.arg.function_args, function (arg, index) {
        if (typeof arg.property_name != 'undefined') {
          $scope.args.push(arg.property_name);
        }
        else if (typeof arg.constant_value != 'undefined') {
          $scope.args.push(arg.constant_value);
        }
      });
      $scope.name = $scope.arg.property_name;
      $scope.value = $scope.arg.constant_value;
    }

    $scope.$watch(function () {
      return [$scope.func,
              $scope.type,
              $scope.name,
              $scope.value,
              $scope.args];
    }, function () {
      var args = [];
      if (!$scope.type) return;
      _.each($scope.args, function (arg, index) {
        var type = $scope.func.options[index].type;
        var cpf = {
          'cpf_type': type
        };
        if (type == 'property') {
          cpf['property_name'] = arg;
        }
        else if (type == 'constant') {
          cpf['constant_value'] = arg;
        }
        args.push(cpf);
      });
      $scope.arg.cpf_type = $scope.type.type;
      $scope.arg.function_name = $scope.func.value;
      $scope.arg.function_args = args;
      $scope.arg.property_name = $scope.name;
      $scope.arg.constant_value = $scope.value;
    }, true);

  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operators/cpf.html',
    controller: controller,
    scope: {
      arg: '=' 
    }
  };
});

qb.directive('op', function ($http, $compile) {
  var linker = function (scope, element, attrs) {
    if (scope.arg) {
      var type = scope.arg;
      scope.type = findObjectInListBasedOnKey(scope.types, 'value', type);
    }
  };

  var controller = ['$scope', function ($scope) {
    $scope.types = [
      {name: 'is less than', value: 'lt'},
      {name: 'is less than or equal to', value: 'lte'},
      {name: 'is greater than', value: 'gt'},
      {name: 'is greater than or equal to', value: 'gte'},
      {name: 'is equal to', value: 'eq'},
      {name: 'contains', value: 'contains'},
      {name: 'is in', value: 'in'},
      {name: 'matches regex', value: 'regex'}
    ];
    $scope.type = $scope.types[0];

    if (!$scope.arg) {
      $scope.arg = $scope.type.value;
    }

    $scope.$watch('type', function () {
      if ($scope.type != undefined) {
        $scope.arg = $scope.type.value;
      }
    });
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operators/op.html',
    controller: controller,
    link: linker,
    scope: {
      arg: '='
    }
  };
});

qb.directive('aggtype', function ($http, $compile) {
  var linker = function (scope, element, attrs) {
    if (scope.arg) {
      var val = scope.arg;
      scope.aggType = findObjectInListBasedOnKey(scope.aggTypes, 'value', val);
    }
  };

  var controller = ['$scope', function ($scope) {
    $scope.aggTypes = [
      {name: 'Minimum', value: 'Min'},
      {name: 'Maximum', value: 'Max'},
      {name: 'Average', value: 'Avg'},
      {name: 'Count', value: 'Count'},
      {name: 'Sum', value: 'Sum'},
      {name: 'Value count', value: 'Valuecount'}
    ];
    $scope.aggType = $scope.aggTypes[3];

    if (!$scope.arg) {
      $scope.arg = {};
    }

    $scope.$watch('aggType', function () {
      if ($scope.aggType != undefined) {
        $scope.arg = $scope.aggType.value;
      }
    });
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operators/aggtype.html',
    controller: controller,
    link: linker,
    scope: {
      arg: '='
    }
  };
});

qb.directive('value', function ($http, $compile) {
  var linker = function (scope, element, attrs) {
    scope.val = scope.arg;
  };

  var controller = ['$scope', function ($scope) {
    $scope.$watch('val', function () {
      $scope.arg = $scope.val;
    });

    if (!$scope.arg) {
      $scope.arg = '';
    }
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operators/input.html',
    controller: controller,
    link: linker,
    scope: {
      arg: '=',
      placeholder: '=?',
      type: '=?'
    }
  };
});

qb.directive('direction', function ($http, $compile) {
  var linker = function (scope, element, attrs) {
    scope.direction = scope.arg;
  };

  var controller = ['$scope', function ($scope) {
    $scope.directions = [
      {name: 'Ascending', type: 'asc'},
      {name: 'Descending', type: 'desc'}
    ];

    $scope.$watch('direction', function () {
      $scope.arg = $scope.direction;
    });

    if (!$scope.arg) {
      $scope.arg = $scope.directions[0];
    }
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operators/direction.html',
    controller: controller,
    link: linker,
    scope: {
      arg: '='
    }
  };
});

qb.directive('property', function ($http, $compile) {
  return {
    restrict: 'E',
    templateUrl: '/static/partials/operators/property.html',
    scope: {
      model: '=',
      panel: '='
    }
  }
});
