angular.module('blueprint')
  .controller('Header', function($scope, $location, store, auth) {
    $scope.navAreas = {
        'STATS':    'stats',         // /stats
        'SCHEMA':   'schema',        // /schema
        'KINESIS':  'kinesisconfig'  // /kinesisconfigs
    };
    $scope.getError = store.getError;
    $scope.clearError = store.clearError;
    $scope.getMessage = store.getMessage;
    $scope.clearMessage = store.clearMessage;
    $scope.loginName = auth.getLoginName();
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
    auth.isEditable($scope);
  });
