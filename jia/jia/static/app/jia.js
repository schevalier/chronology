var jia = angular.module('jia', [
  'ngRoute',
  'jia.editboard'
]);

jia.config(['$routeProvider',
  function ($routeProvider) {
    $routeProvider.
      when('/boards/:boardId', {
        templateUrl: '/static/app/editboard/editboard.html',
        controller: 'BoardController'
      }).
      otherwise({
        redirectTo: '/boards/new'
      });
  }
]);

jia.run(['$rootScope', function($rootScope) {
  // Make Object available in templates
  $rootScope.Object = Object;
}]);

