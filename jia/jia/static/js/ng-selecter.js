/*
 * Simple Angular directive for Ben Plum's Selecter.js
 *
 * In controller:
 *
 * $scope.choices = {
 *   'Option 1': {
 *      anythingElse: 39353
 *   },
 *   'Option 2': {
 *      anythingElse: 40406,
 *      selected: true
 *   },
 *   'Option 3': {
 *      anythingElse: 35353
 *   }
 * };
 * 
 * // You can make optgroups:
 * $scope.choices = {
 *   'Option 1': {
 *      anythingElse: 39353,
 *      group: 'Group 1'
 *   },
 *   'Option 2': {
 *      anythingElse: 40406,
 *      group: 'Group 2'
 *   },
 *   'Option 3': {
 *      anythingElse: 35353,
 *      group: 'Group 2'
 *   }
 * };
 * 
 * $scope.chosen = $scope.choices[0];
 *
 * $scope.enabled = true;
 * 
 * // Include any of the selecter config options
 * $scope.optionalConfig = {
 *   customClass: 'fancy',
 *   label: 'Choose an option...'
 * }
 * 
 * In view:
 * <selecter model="chosen"
 *           options="choices"
 *           disabled="false"
 *           config="optionalConfig">
 * </selecter>
 *
 */

angular.module('selecter', [])

.directive('selecter', ['$http', '$compile', function ($http, $compile) {
  var linker = function(scope, element, attrs) {
    var changeCallback = function (value, index) {
      scope.model = lookupOption(value);
      scope.$apply();
    }

    var lookupOption = function (value) {
      var options;
      if (scope.options && typeof scope.options.length != 'undefined') {
        optGroups = false;
        options = {
          options: scope.options
        };
      }
      else {
        options = scope.options;
      }
      for (var key in options) {
        if (options.hasOwnProperty(key)) {
          for (var i = 0; i < options[key].length; i++) {
            if (options[key][i].name == value) {
              return options[key][i];
            }
          }
        }
      }
    }

    var createSelecter = function (selectedOption) {
      $(element).html('');
      var select = $('<select></select>');
      var optGroups = true;
      var options;
      
      if (scope.disabled) {
        select.attr('disabled', 'disabled');
      }
      $(element).append(select);
      
      if (scope.options && typeof scope.options.length != 'undefined') {
        optGroups = false;
        options = {
          options: scope.options
        };
      }
      else {
        options = scope.options;
      }

      for (var key in options) {
        if (options.hasOwnProperty(key)) {
          var appendTo;
          if (optGroups) {
            appendTo = $('<optgroup></optgroup>').attr('label', key);
            select.append(appendTo);
          }
          else {
            appendTo = select;
          }
          for (var i = 0; i < options[key].length; i++) {
            var option = $('<option></option>');
            var optionAttrs = options[key][i];
            option.text(optionAttrs.name);
            option.attr('value', optionAttrs.name);
            if (optionAttrs.disabled) {
              option.attr('disabled', 'disabled');
            }
            if (selectedOption &&
                selectedOption.name == optionAttrs.name) {
              option.attr('selected', 'selected');
            }
            appendTo.append(option);
          }
        }
      }
      if (typeof scope.config == 'object') {
        scope.config.callback = changeCallback;
      }
      else {
        scope.config = {
          callback: changeCallback
        }
      }
      select.selecter(scope.config);
    }
     
    var updateSelector = function (newVal) {
      // Update the selecter when the value changes in scope
      // Selecter doesn't provide an update method, so destroy and recreate
      $(element).find('select').selecter('destroy');
      createSelecter(newVal);
    };
 
    scope.$watch('model', function (newVal, oldVal) {
      // The timeout of zero is magic to wait for an ng-repeat to finish
      // populating the <select>. See: http://stackoverflow.com/q/12240639/
      setTimeout(function () { updateSelector(newVal); });
    });

    scope.$on('$destroy', function() {
      $(element).find('select').selecter('destroy');
    });
  }

  return {
    restrict: "E",
    replace: false,
    link: linker,
    scope: {
      model: '=',
      options: '=',
      config: '=?',
      disabled: '=?',
      sid: '=?'
    }
  };
}]);
