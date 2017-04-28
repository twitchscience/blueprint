angular.module('blueprint')
  .controller('ListKinesisConfigs', function($scope, $location, KinesisConfig, store, auth) {
    $scope.loginName = auth.getLoginName();
    $scope.isAdmin = auth.isAdmin();
    $scope.isEditable = false;
    auth.isEditableContinuation(function(isEditable) {
      $scope.isEditable = isEditable;
    });
    $scope.loading = true;
    $scope.ready = false;
    KinesisConfig.all(function(data) {
      $scope.showMaintenance = false;
      $scope.loading = false;
      $scope.kinesisconfigs = data;
    }, function(err) {
      $scope.loading = false;
      var msg;
      if (err.data) {
        msg = err.data;
      } else {
        msg = 'Error loading Kinesis configs:' + err;
      }
      store.setError(msg);
    });
  });
