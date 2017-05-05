angular.module('blueprint')
  .controller('ShowKinesisConfig', function ($scope, $http, $location, $routeParams, $q, store, KinesisConfig, auth) {
    var kinesisconfig, dropMessage, cancelDropMessage;
    $scope.loading = true;
    $scope.loginName = auth.getLoginName();
    $scope.isAdmin = auth.isAdmin();

    var kinesisconfigRequest = KinesisConfig.get($routeParams, function(data) {
      if (data) {
        kinesisconfig = data;
      }
    }, function(err) {
      var msg;
      if (err.data) {
        msg = 'API Error: ' + err.data;
      } else {
        msg = 'Kinesis config not found or threw an error';
      }
      store.setError(msg, '/kinesisconfigs');
    }).$promise;


    $q.all([kinesisconfigRequest]).then(function() {
      if (!kinesisconfig) {
        store.setError('API Error', '/kinesisconfigs');
      }
      $scope.loading = false;
      $scope.showDropConfig = false;
      $scope.dropConfigReason = '';
      $scope.executingDrop = false;
      $scope.dropMessage = 'Drop Config';
      $scope.cancelDropMessage = 'Cancel Drop';
      $scope.successDropMessage = 'Kinesis Config Dropped';
      $scope.kinesisconfig = kinesisconfig;
      try {
        $scope.configJSON = JSON.stringify(kinesisconfig.SpadeConfig, null, 2)
      } catch (err) {
        store.setError("Could not stringify JSON from server: " + err)
      }
      $scope.StreamName = kinesisconfig.SpadeConfig.StreamName
      $scope.StreamType = kinesisconfig.SpadeConfig.StreamType
      $scope.AWSAccount = kinesisconfig.AWSAccount

      $scope.updateKinesisConfig = function() {
        try {
          $scope.kinesisconfig.SpadeConfig = JSON.parse($scope.configJSON)
        } catch (err) {
          store.setError("Invalid JSON - could not be parsed: " + err)
          return false
        }
        if ($scope.StreamName != kinesisconfig.SpadeConfig.StreamName ||
            $scope.StreamType != kinesisconfig.SpadeConfig.StreamType ||
            $scope.AWSAccount != kinesisconfig.AWSAccount) {
          store.setError("AWS account, stream name and stream type must not be changed")
          return false
        }
        KinesisConfig.update(
          {account: kinesisconfig.AWSAccount, type: kinesisconfig.SpadeConfig.StreamType, name: kinesisconfig.SpadeConfig.StreamName},
          {kinesisconfig: kinesisconfig},
          function() {
            store.setMessage("Succesfully updated Kinesis configuration: " +  kinesisconfig.SpadeConfig.StreamName);
          },
          function(err) {
            store.setError(err, undefined);
          });
      };
      $scope.dropConfig = function() {
        if ($scope.dropConfigReason === '') {
          store.setError("Please enter a reason for dropping the Kinesis configuration");
          return false
        }
        $scope.executingDrop = true;
        KinesisConfig.drop(
          { StreamName: $scope.StreamName,
            StreamType: $scope.StreamType,
            AWSAccount: $scope.AWSAccount,
            Reason: $scope.dropConfigReason},
          function() {
            store.setMessage($scope.successDropMessage);
            $location.path('/kinesisconfigs');
            $scope.executingDrop = false;
          },
          function(err) {
            store.setError(err, undefined);
            $scope.executingDrop = false;
          });
      };
    });
  });
