angular.module('blueprint.components.auth', [
  'ngCookies',
  'blueprint.components.rest',
  'blueprint.components.store'
]).service('Auth', function($cookies, Store, Maintenance) {
    var loginName = $cookies.get('displayName');
    var isAdmin = ($cookies.get('isAdmin') === "true");

    var loginError = $cookies.get('loginError');
    $cookies.remove('loginError');
    if (loginError !== "") {
      Store.setError(loginError)
    }

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
          f(!data.is_maintenance, data.user);
        }, function(err) {
          Store.setError('Error loading maintenance mode: ' + err);
          f(false);
        });
      },
      globalIsEditable: function(scope) {
        scope.globalIsEditable = false;
        this.globalIsEditableContinuation(function(globalIsEditable, user) {
          scope.globalIsEditable = globalIsEditable;
          scope.globalMaintenanceModeUser = user;
        });
      }
    };
  });
