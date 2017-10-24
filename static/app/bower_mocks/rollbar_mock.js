// This is a mock rollbar library so tests will load
(function(angular){
  angular.module('tandibar/ng-rollbar', []);
  angular.module('tandibar/ng-rollbar').config(['$provide', function($provide) {
    $provide.decorator('$exceptionHandler', ['$delegate', '$window', function($delegate, $window) {
      return function (exception, cause) {
        if($window.Rollbar) {
          $window.Rollbar.error(exception, {cause: cause});
        }
        $delegate(exception, cause);
      };
    }]);
  }]);
  angular.module('tandibar/ng-rollbar').provider('Rollbar', function RollbarProvider() {
    this.init = function(config) { };
    this.deinit = function () { };
    this.$get = function(){
      return {
        Rollbar: {
          error: function(){}
        }
      };
    };

  });
})
(angular);
