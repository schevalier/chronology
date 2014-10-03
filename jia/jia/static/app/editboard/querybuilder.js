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
    templateUrl: '/static/app/editboard/querybuilder.html',
    controller: controller,
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
  var linker = function (scope, element, attrs, ngModel) {
    if (scope.step.operation) {
      scope.$watch(function () {
        return scope.step.operation;
      }, function (newVal, oldVal) {
        $http.get(['static/app/editboard/operators',
                   scope.step.operation.operator + '.html'].join('/'))
          .success(function(data, status, headers, config) {
            $(element).find('div.args').html(data);
            $compile(element.contents())(scope);
          });
      });
    }
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

    // If the step is not the "New operation..." add control, then it will
    // have an operation
    if ($scope.step.operation) {
      // Keep track of all new schema properties created during this step
      $scope.step.fields = [];
    
      // Initialize the output schema
      $scope.schemas[$scope.$index + 1] = {}; 

      // Changes to schemaIn require an update to the output schema in index+1
      $scope.$watch(function () {
        return $scope.schemas[$scope.$index];
      }, function (newVal, oldVal) {
        $scope.schemas[$scope.$index] = newVal;
        $scope.updateSchema();
      });

      // Any change to fields created in this query step requires an update
      $scope.$watch('step.fields', function () {
        $scope.updateSchema();
      }, true);

      $scope.addOperand = function (operands) {
        operands.push({});
      };

      $scope.removeOperand = function (idx, operands) {
        operands.splice(idx, 1);
      }

      $scope.replaceSchema = function () {
        /*
         * Create ouptut schema set containing only the new properties defined
         * in this query step
         */
        var schemaOut = {};
        for (var field in $scope.step.fields) {
          schemaOut[$scope.step.fields[field]] = true;
        }
        $scope.schemas[$scope.$index + 1] = schemaOut;
      };

      $scope.mergeSchema = function () {
        /*
         * Create output schema set containing the properties in the input
         * schema set in addition to the new properties defined in this query
         * step.
         */
        var schemaOut = angular.extend({}, $scope.schemas[$scope.$index]);
        for (var field in $scope.step.fields) {
          schemaOut[$scope.step.fields[field]] = true;
        }
        $scope.schemas[$scope.$index + 1] = schemaOut;
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
          $scope.schemas[$scope.index + 1] = $scope.schemas[$scope.$index];
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
    }
  }];

  return {
    restrict: "E",
    templateUrl: '/static/app/editboard/step.html',
    controller: controller,
    link: linker,
    scope: true
  };
});

qb.directive('cpf', function () {
  /*
   * Constant/property/function
   *
   * See top of file for detailed information on the structure of the CPF
   * model.
   *
   */
  var linker = function (scope, elem, attrs, ngModel) {
    // If no ng-model was supplied then nothing needs to be done.
    if (!ngModel) return;

    // Build a CPF data structure and upate the ng-model when the user changes
    // any of the inputs.
    scope.$watch(function () {
      return [scope.func,
              scope.type,
              scope.name,
              scope.value,
              scope.args];
    }, function () {
      var args = [];
      if (!scope.type) return;
      _.each(scope.args, function (arg, index) {
        var type = scope.func.options[index].type;
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
      var newVal = {
        cpf_type: scope.type.type,
        function_name: scope.func.value,
        function_args: args,
        property_name: scope.name,
        constant_value: scope.value
      };
      ngModel.$setViewValue(newVal);
    }, true);

    var model = ngModel.$modelValue;
    if (!model) {
      ngModel.$setViewValue({});
    }
    else if (model.cpf_type) {
      scope.type = findObjectInListBasedOnKey(scope.types, 'type',
                                              model.cpf_type);
      scope.func = findObjectInListBasedOnKey(scope.functions, 'value',
                                               model.function_name);
      _.each(model.function_args, function (arg, index) {
        if (typeof model.property_name != 'undefined') {
          scope.args.push(arg.property_name);
        }
        else if (typeof model.constant_value != 'undefined') {
          scope.args.push(arg.constant_value);
        }
      });
      scope.name = model.property_name;
      scope.value = model.constant_value;
    }
  };

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
  }];

  return {
    restrict: "E",
    templateUrl: '/static/app/editboard/inputs/cpf.html',
    controller: controller,
    link: linker,
    scope: true,
    require: '?ngModel'
  };
});

qb.directive('op', function () {
  /*
   * Basic filter operator type select element (lt, gt, gte, eq, etc)
   */
  var linker = function (scope, element, attrs, ngModel) {
    if (!ngModel) return;

    if (ngModel.$modelValue) {
      var type = ngModel.$modelValue;
      scope.type = findObjectInListBasedOnKey(scope.types, 'value', type);
    }
    else {
      ngModel.$setViewValue(scope.type.value);
    }

    scope.$watch('type', function () {
      if (scope.type != undefined) {
        ngModel.$setViewValue(scope.type.value);
      }
    });
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
  }];

  return {
    restrict: "E",
    templateUrl: '/static/app/editboard/inputs/op.html',
    controller: controller,
    link: linker,
    require: '?ngModel',
    scope: true
  };
});

qb.directive('aggtype', function () {
  /*
   * Provides a dropdown list of aggregation types
   */
  var linker = function (scope, element, attrs, ngModel) {
    if (!ngModel) return;

    if (ngModel.$modelValue) {
      var val = ngModel.$modelValue;
      scope.aggType = findObjectInListBasedOnKey(scope.aggTypes, 'value', val);
    }
    else {
      ngModel.$setViewValue({});
    }

    scope.$watch('aggType', function () {
      if (scope.aggType != undefined) {
        ngModel.$setViewValue(scope.aggType.value);
      }
    });
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
  }];

  return {
    restrict: "E",
    templateUrl: '/static/app/editboard/inputs/aggtype.html',
    controller: controller,
    link: linker,
    require: '?ngModel',
    scope: true
  };
});

qb.directive('value', function () {
  /*
   * Thin wrapper around <input>
   *
   * :param placeholder: (optional) Placeholder text for <input>
   * :param type: (optional) TODO(derek): Not implemented
   * :param altersSchema: (optional) Indicates that this value is creating a
   * new property on the schema
   *
   * Example:
   * <value alters-schema placeholder="New field" type="text"></value>
   *
   */

  // Behaves statically because a directive's factory function is invoked only
  // once, when the compiler matches the directive for the first time.
  var id = 0;
  
  var linker = function (scope, element, attrs, ngModel) {
    if (attrs['placeholder']) {
      $(element).find('input').attr('placeholder', attrs['placeholder']);
    }

    if (ngModel) {
      if (ngModel.$modelValue) {
        scope.val = ngModel.$modelValue;
      }
      else {
        ngModel.$setViewValue('');
      }

      // Get unique ID from incrementing static `id`
      var uid = id++;
      scope.$watch('val', function () {
        ngModel.$setViewValue(scope.val);
        if (attrs['alters-schema']) {
          scope.step.fields[uid] = scope.val;
        }
      });
    }
  };
 
  return {
    restrict: "E",
    templateUrl: '/static/app/editboard/inputs/input.html',
    link: linker,
    scope: true,
    require: '?ngModel'
  };
});

qb.directive('direction', function () {
  /*
   * Sort order (asc/desc) select element
   */
  var linker = function (scope, element, attrs, ngModel) {
    scope.directions = [
      {name: 'Ascending', type: 'asc'},
      {name: 'Descending', type: 'desc'}
    ];
    
    if (ngModel.$viewValue) {
      scope.direction = ngModel.$viewValue;
    }
    else {
      scope.direction = scope.directions[0];
    }

    scope.$watch('direction', function () {
      ngModel.$setViewValue(scope.direction);
    });
  };

  return {
    restrict: "E",
    templateUrl: '/static/app/editboard/inputs/direction.html',
    link: linker,
    scope: true,
    require: '?ngModel'
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
      if (newVal) {
        $scope.properties = Object.keys($scope.schema);
      }
    });
  }];
  return {
    restrict: 'E',
    templateUrl: '/static/app/editboard/inputs/property.html',
    controller: controller,
    scope: {
      model: '=',
      schema: '='
    }
  }
});
