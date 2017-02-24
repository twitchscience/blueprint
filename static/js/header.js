angular.module('blueprint')
  .controller('Header', function($scope, $location, store, auth) {
    $scope.getError = store.getError;
    $scope.clearError = store.clearError;
    $scope.getMessage = store.getMessage;
    $scope.clearMessage = store.clearMessage;
    $scope.loginName = auth.getLoginName();
    $scope.loc = $location;
    auth.isEditable($scope);
  });
