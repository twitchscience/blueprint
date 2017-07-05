angular.module('blueprint')
  .controller('ListSchemas', function($scope, $location, Schema, Suggestions, Maintenance, store, auth) {
    $scope.loginName = auth.getLoginName();
    $scope.isAdmin = auth.isAdmin();
    $scope.isEditable = false;
    auth.globalIsEditableContinuation(function(globalIsEditable, user) {
      $scope.globalIsEditable = globalIsEditable;
      $scope.globalMaintenanceModeUser = user;
      if (globalIsEditable) {
        $scope.maintenanceDirection = "on";
      } else {
        $scope.maintenanceDirection = "off";
      }
    });
    $scope.loading = true;
    $scope.ready = false;
    Schema.all(function(data) {
      $scope.showMaintenance = false;
      $scope.loading = false;
      $scope.schemas = data;
      var existingSchemas = {};
      angular.forEach($scope.schemas, function(s) {
        existingSchemas[s.EventName] = true;
      });

      Suggestions.all(function(data) {
        $scope.loading = false;
        $scope.ready = true;
        $scope.suggestions = [];
        angular.forEach(data, function(s) {
          if (!existingSchemas[s.EventName]) {
            $scope.suggestions.push(s);
          }
        });
      });
    }, function(err) {
      $scope.loading = false;
      var msg;
      if (err.data) {
        msg = err.data;
      } else {
        msg = 'Error loading schemas:' + err;
      }
      store.setError(msg);
    });
    $scope.toggleMaintenanceMode = function() {
      if (!$scope.toggleMaintenanceModeReason) {
        store.setError("Please enter a reason for turning maintenance mode " + $scope.maintenanceDirection);
        return
      }
      $scope.togglingMaintenanceMode = true;
      Maintenance.post(
        {is_maintenance: $scope.globalIsEditable,
         reason: $scope.toggleMaintenanceModeReason},
        function() {
          store.setMessage("Maintenance mode turned " + $scope.maintenanceDirection);
          $location.path('/schemas');
          $scope.globalIsEditable = !$scope.globalIsEditable;
          $scope.globalMaintenanceModeUser = $scope.loginName;
          $scope.maintenanceDirection = $scope.globalIsEditable ? "on" : "off";
          $scope.showMaintenance = false;
          $scope.togglingMaintenanceMode = false;
        },
        function(err) {
          store.setError(err, undefined);
          $scope.showMaintenance = false;
          $scope.togglingMaintenanceMode = false;
        });
    };
  });
