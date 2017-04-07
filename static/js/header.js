angular.module('blueprint')
  .controller('Header', function($scope, $location, store, auth) {
    $scope.getError = store.getError;
    $scope.clearError = store.clearError;
    $scope.getMessage = store.getMessage;
    $scope.clearMessage = store.clearMessage;
    $scope.loginName = auth.getLoginName();
    $scope.loc = $location;
    $scope.currentEditor = 'none';
    $scope.currentEditor = $location.$$url.indexOf('/schema') == 0 ? 'schema' : $scope.currentEditor
    $scope.currentEditor = $location.$$url.indexOf('/kinesisconfig') == 0 ? 'kinesisconfig' : $scope.currentEditor
    $scope.updateEditor = function(newEditor) {
      $scope.currentEditor = newEditor;
    };
    auth.isEditable($scope);
  });
