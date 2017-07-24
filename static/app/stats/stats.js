angular.module('blueprint.stats', ['blueprint.components.rest'])
  .controller('Analytics', function($scope, Stats) {
    $scope.loading = true;
    Stats.get(function(data) {
      $scope.loading = false;
      $scope.activeUsers = data.ActiveUsers;
      $scope.dailyChanges = data.DailyChanges;
    }, function(err) {
      $scope.loading = false;
      var msg;
      if (err.data) {
        msg = err.data;
      } else {
        msg = 'Error loading stats:' + err;
      }
      store.setError(msg);
    });
  });
