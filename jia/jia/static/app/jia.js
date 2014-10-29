var jia = angular.module('jia', [
  'ngRoute',
  'jia.board',
  'jia.boardlist'
]);

jia.config(['$routeProvider',
  function ($routeProvider) {
    $routeProvider.
      when('/boards', {
        templateUrl: '/static/app/boardlist/boardlist.html',
        controller: 'BoardListController'
      }).
      when('/boards/:boardId', {
        templateUrl: '/static/app/board/board.html',
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

