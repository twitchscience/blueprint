angular.module('blueprint.components.store', [])
  .service('Store', function($location) {
    var data = {
      event: undefined,
      message: undefined,
      error: undefined,
    };

    return {
      setEvent: function(ev) {
        data.event = ev;
      },

      getEvent: function() {
        return data.event;
      },

      setError: function(err, path) {
        data.error = err;
        if (path) {
          $location.path(path);
        }
      },

      getError: function() {
        return data.error;
      },

      clearError: function() {
        data.error = undefined;
      },

      setMessage: function(msg) {
        data.message = msg;
      },

      getMessage: function() {
        return data.message;
      },

      clearMessage: function() {
        data.message = undefined;
      },
    };
  });
