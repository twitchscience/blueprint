angular.module('blueprint.kinesisconfig.list', [
  'blueprint.components.auth',
  'blueprint.components.rest',
  'blueprint.components.store'
]).controller('ListKinesisConfigs', function($scope, $location, KinesisConfig, Store, Auth) {
    $scope.loginName = Auth.getLoginName();
    $scope.isAdmin = Auth.isAdmin();

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
      Store.setError(msg);
    });
  });
