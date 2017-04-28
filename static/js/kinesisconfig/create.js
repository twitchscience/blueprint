angular.module('blueprint')
  .controller('CreateKinesisConfig', function($scope, $location, $routeParams, store, KinesisConfig, auth) {
    $scope.loginName = auth.getLoginName();
    auth.isEditable($scope);

    $scope.StreamName = '';
    $scope.StreamType = '';
    $scope.AWSAccount = 0;
    $scope.Team = '';
    $scope.Contact = '';
    $scope.Usage = '';
    $scope.Consuminglibrary = '';
    $scope.SpadeConfig = '';
    $scope.configJSON = ''
    $scope.createKinesisConfig = function() {
      store.clearError();
      try {
        $scope.SpadeConfig = JSON.parse($scope.configJSON)
      } catch (err) {
        store.setError("Invalid JSON - could not be parsed: " + err)
        return false
      }
      if (!$scope.SpadeConfig.StreamName || !$scope.SpadeConfig.StreamType || $scope.AWSAccount == 0) {
        store.setError("AWS account, stream name and stream type must be present")
        return false
      }
      KinesisConfig.put({
        "StreamName": $scope.SpadeConfig.StreamName,
        "StreamType": $scope.SpadeConfig.StreamType,
        "AWSAccount": $scope.AWSAccount,
        "Team": $scope.Team,
        "Contact": $scope.Contact,
        "Usage": $scope.Usage,
        "ConsumingLibrary": $scope.ConsumingLibrary,
        "SpadeConfig": $scope.SpadeConfig
      }, function() {
        store.setMessage("Succesfully created Kinesis config: " + $scope.StreamName)
        $location.path('/kinesisconfigs');
      }, function(err) {
        var msg;
        if (err.data) {
          msg = err.data;
        } else {
          msg = 'Error creating Kinesis Config:' + err;
        }
        store.setError(msg);
        return false;
      });
    };
  });
