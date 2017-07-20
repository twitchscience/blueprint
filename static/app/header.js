angular.module('blueprint')
  .controller('Header', function($scope, $location, Store, Auth) {
    $scope.navAreas = {
        'STATS':    'stats',         // /stats
        'SCHEMA':   'schema',        // /schema
        'KINESIS':  'kinesisconfig'  // /kinesisconfigs
    };
    $scope.getError = Store.getError;
    $scope.clearError = Store.clearError;
    $scope.getMessage = Store.getMessage;
    $scope.clearMessage = Store.clearMessage;
    $scope.loginName = Auth.getLoginName();
    $scope.urlLocation = $location.$$url;
    $scope.currentNavArea = (function(url) {
        if (url.indexOf('/stats') == 0) {
            return $scope.navAreas.STATS;
        } else if (url.indexOf('/kinesisconfig') == 0) {
            return $scope.navAreas.KINESIS;
        } else {
            return $scope.navAreas.SCHEMA;
        }
    })($scope.urlLocation);
    $scope.updateNavArea = function(newNavArea) {
      $scope.currentNavArea = newNavArea;
    };
    Auth.globalIsEditable($scope);
  });
