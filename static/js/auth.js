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
      isEditableContinuation: function(f) {
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
      isEditable: function(scope) {
        scope.isEditable = false;
        this.isEditableContinuation(function(isEditable) {
          scope.isEditable = isEditable;
        });
      }
    };
  });
