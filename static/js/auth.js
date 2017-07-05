angular.module('blueprint')
  .service('auth', function($cookies, Maintenance) {
    var loginName = $cookies.get('displayName');
    var isAdmin = ($cookies.get('isAdmin') === "true");
    return {
      getLoginName: function() {
        return loginName;
      },
      isAdmin: function() {
        return isAdmin;
      },
      globalIsEditableContinuation: function(f) {
        if (!loginName) {
          f(false);
          return;
        }
        Maintenance.get(function(data) {
          f(!data.is_maintenance);
        }, function(err) {
          store.setError('Error loading maintenance mode: ' + err);
          f(false);
        });
      },
      globalIsEditable: function(scope) {
        scope.globalIsEditable = false;
        this.globalIsEditableContinuation(function(globalIsEditable) {
          scope.globalIsEditable = globalIsEditable;
        });
      }
    };
  });
