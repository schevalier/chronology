angular.module('jia.navigation', [])

.controller('NavigationController', ['$scope', 'ToolbarButtonService',
function ($scope, ToolbarButtonService) {
  $scope.user = JIA_USER;
  $scope.toolbarButtons = [];
  $scope.$watch(function () {
    return ToolbarButtonService.getButtons();
  },
  function (newVal, oldVal) {
    $scope.toolbarButtons = newVal;
  });
}])

.factory('ToolbarButtonService', ['$rootScope', function ($rootScope) {
  var buttons = [];
  var service = {};

  $rootScope.$on('$routeChangeStart', function () {
    buttons = [];
  });

  service.getButtons = function () {
    return buttons;
  };

  service.setButtons = function (newButtons) {
    buttons = newButtons;
  };

  return service;
}]);
