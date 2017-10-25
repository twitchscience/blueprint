// This is a mock rollbar library so tests will load
(function(angular){
  angular.module('tandibar/ng-rollbar', []);
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
