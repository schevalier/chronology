var boardlist = angular.module('jia.boardlist', ['jia.board']);

boardlist.controller('BoardListController', ['$scope', 'BoardService',
function ($scope, BoardService) {
  $scope.boards = [];
  $scope.getBoards = function () {
    BoardService.getBoards().then(function(boards) {
      $scope.boards = boards;
    });
  };
  $scope.getBoards();
}]);
