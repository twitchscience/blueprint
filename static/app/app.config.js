angular.module('blueprint')
  .config(function($routeProvider) {
    $routeProvider
      .when('/schemas', {
        controller: 'ListSchemas',
        templateUrl: 'app/schema/list/list.html'
      })
      .when('/schema', {
        controller: 'CreateSchema',
        templateUrl: 'app/schema/create/create.html'
      })
      .when('/schema/:scope', {
        controller: 'ShowSchema',
        templateUrl: 'app/schema/show/show.html'
      })
      .when('/kinesisconfigs', {
        controller: 'ListKinesisConfigs',
        templateUrl: 'app/kinesisconfig/list/list.html'
      })
      .when('/kinesisconfig', {
        controller: 'CreateKinesisConfig',
        templateUrl: 'app/kinesisconfig/create/create.html'
      })
      .when('/kinesisconfig/:account/:type/:name', {
        controller: 'ShowKinesisConfig',
        templateUrl: 'app/kinesisconfig/show/show.html'
      })
      .when('/stats', {
        controller: 'Analytics',
        templateUrl: 'app/stats/stats.html'
      })
      .otherwise({
        redirectTo: '/schemas'
      });
  }).config(['$showdownProvider', function($showdownProvider) {
    showdown.setFlavor('github');
    $showdownProvider.setOption('requireSpaceBeforeHeadingText', false);
    $showdownProvider.setOption('simpleLineBreaks', true);
  }]);
