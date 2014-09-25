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
 *
 * Schemas
 * -------
 * Keeping track of the schema is necessary for offering autocomplete on
 * property names. The schema must be flowed through the query, as each step
 * in the query has the ability to modify the schema for future steps. For
 * example, a transform operation adds a new property to the schema.
 *
 * Schemas are stored as object keys to emulate the behavior of sets.
 *
 * sample_schema = {"property1": true, "property2": true};
 *
 * `schemas[0]` is fed by `panel.cache.streamProperties`. The schema is then
 * flowed through all the steps of the query. Each step takes in
 * `schemas[index]`, makes modifications if necessary, and outputs results in
 * `schemas[index + 1]`. Because each step `$watch`es its input, and each
 * step's input is the previous step's output, a change anywhere will
 * immediately flow through all the following steps.
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

qb.directive('querybuilder', function () {
  var controller = ['$scope', function($scope) {
    $scope.nextStep = {};
    $scope.query = $scope.panel.data_source.query;

    // Provide an empty schema set for `schemas[0]` as a placeholder until the
    // AJAX call to `infer_schema` returns a result.
    $scope.schemas = [{}];

    // Allow access from other places (like settings panel)
    $scope.panel.cache.schemas = $scope.schemas;

    $scope.$watch('panel.cache.streamProperties', function (newVal, oldVal) {
      if (newVal) {
        $scope.schemas[0] = {};
        for (var i = 0; i < newVal.length; i++) {
          $scope.schemas[0][newVal[i]] = true;
        }
      }
    });

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
  /*
   * Single query step
   * 
   * :param step: Reference to step model
   * :param schemas: Reference to schemas list for query (schema is different
   * at each step)
   * :param index: Zero-based index of this step in the query
   * :param newop: Boolean specifying if this is an active query step or a
   * "New Operation..." select element.
   */
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

    // Keep track of all new fields created during this step
    $scope.step.fields = [];

    if (typeof $scope.schemas != 'undefined') {
      $scope.schemaIn = $scope.schemas[$scope.index];
      $scope.schemas[$scope.index + 1] = {}; 

      // Changes to schemaIn require an update to the output schema in index+1
      $scope.$watch('schemaIn', function (newVal, oldVal) {
        $scope.schemaIn = newVal;
        $scope.updateSchema();
      });

      // Any change to fields created in this query step requires an update
      $scope.$watch('step.fields', function () {
        $scope.updateSchema();
      }, true);
    }

    $scope.addOperand = function (operands) {
      operands.push({});
    };

    $scope.removeOperand = function (idx, operands) {
      operands.splice(idx, 1);
    }

    $scope.replaceSchema = function () {
      /*
       * Create ouptut schema set containing only the new properties defined in
       * this query step
       */
      var schemaOut = {};
      for (var field in $scope.step.fields) {
        schemaOut[$scope.step.fields[field]] = true;
      }
      $scope.schemas[$scope.index + 1] = schemaOut;
    };

    $scope.mergeSchema = function () {
      /*
       * Create output schema set containing the properties in the input schema
       * set in addition to the new properties defined in this query step.
       */
      var schemaOut = angular.extend({}, $scope.schemaIn);
      for (var field in $scope.step.fields) {
        schemaOut[$scope.step.fields[field]] = true;
      }
      $scope.schemas[$scope.index + 1] = schemaOut;
    };
    
    $scope.updateSchema = function () {
      /*
       * Select schema transform type based on the type of operation being
       * performed in this step.
       */
      if ($scope.step.operation.operator == 'aggregate') {
        $scope.replaceSchema();
      }
      else if ($scope.step.operation.operator == 'transform') {
        $scope.mergeSchema();
      }
      else {
        $scope.schemas[$scope.index + 1] = $scope.schemaIn;
      }
    };

    // If the operation type changes, the output schema needs an update
    $scope.$watch(function () {
      return $scope.step.operation;
    }, function (newVal, oldVal) {
      if (newVal) {
        $scope.updateSchema();
      }
    });
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/step.html',
    controller: controller,
    link: linker,
    scope: {
      step: '=',
      schemas: '=',
      index: '=',
      newop: '='
    }
  };
});

qb.directive('cpf', function () {
  /*
   * Constant/property/function
   *
   * See top of file for detailed information on the structure of the CPF
   * model.
   *
   * :param arg: CPF model
   */
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

qb.directive('op', function () {
  /*
   * Basic filter operator type select element
   *
   * :param arg: Model to store operator
   */
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

qb.directive('aggtype', function () {
  /*
   * Provides a dropdown list of aggregation types
   *
   * :param arg: Model to store selected value
   */
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

qb.directive('value', function () {
  /*
   * Thin wrapper around <input>
   *
   * :param arg: Model to update with <input> contents
   * :param altersSchema: (optional) Contains a reference to a query `step` if
   * the directive is being used to add a property to the schema.
   * :param placeholder: (optional) Placeholder text for <input>
   * :param type: (optional) TODO(derek): Not implemented
   *
   */
  var linker = function (scope, element, attrs) {
    scope.val = scope.arg;
  };
 
  // Behaves statically because a directive's factory function is invoked only
  // once, when the compiler matches the directive for the first time.
  var id = 0;
  var controller = ['$scope', function ($scope) {
    // Get unique ID from incrementing static `id`
    var uid = id++;
    $scope.$watch('val', function () {
      $scope.arg = $scope.val;
      if ($scope.altersSchema) {
        $scope.altersSchema.fields[uid] = $scope.val;
      }
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
      altersSchema: '=?',
      placeholder: '=?',
      type: '=?'
    }
  };
});

qb.directive('direction', function () {
  /*
   * Sort order (asc/desc) select element
   *
   * :param arg: Model to store direction
   */
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

qb.directive('property', function () {
  /*
   * Autocomplete property name field
   *
   * :param model: Model to store property name in
   * :param schema: Schema set to extract keys from
   */
  var controller = ['$scope', function ($scope) {
    $scope.$watch('schema', function (newVal, oldVal) {
      $scope.properties = Object.keys($scope.schema);
    });
  }];
  return {
    restrict: 'E',
    templateUrl: '/static/partials/operators/property.html',
    controller: controller,
    scope: {
      model: '=',
      schema: '='
    }
  }
});
