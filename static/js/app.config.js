angular.module('blueprint')
  .config(function($routeProvider) {
    $routeProvider
      .when('/schemas', {
        controller: 'ListSchemas',
        templateUrl: 'template/schema/list.html'
      })
      .when('/schema', {
        controller: 'CreateSchema',
        templateUrl: 'template/schema/create.html'
      })
      .when('/schema/:scope', {
        controller: 'ShowSchema',
        templateUrl: 'template/schema/show.html'
      })
      .when('/kinesisconfigs', {
        controller: 'ListKinesisConfigs',
        templateUrl: 'template/kinesisconfig/list.html'
      })
      .when('/kinesisconfig', {
        controller: 'CreateKinesisConfig',
        templateUrl: 'template/kinesisconfig/create.html'
      })
      .when('/kinesisconfig/:account/:type/:name', {
        controller: 'ShowKinesisConfig',
        templateUrl: 'template/kinesisconfig/show.html'
      })
      .when('/stats', {
        controller: 'Analytics',
        templateUrl: 'template/stats.html'
      })
      .otherwise({
        redirectTo: '/schemas'
      });
  }).config(['$showdownProvider', function($showdownProvider) {
    showdown.setFlavor('github');
    $showdownProvider.setOption('requireSpaceBeforeHeadingText', false);
    $showdownProvider.setOption('simpleLineBreaks', true);
  }]);