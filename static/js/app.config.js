angular.module('blueprint')
  .config(function($routeProvider) {
    $routeProvider
      .when('/schemas', {
        controller: 'List',
        templateUrl: 'template/schema/list.html'
      })
      .when('/schema', {
        controller: 'Create',
        templateUrl: 'template/schema/create.html'
      })
      .when('/schema/:scope', {
        controller: 'Show',
        templateUrl: 'template/schema/show.html'
      })
      .when('/stats', {
        controller: 'Analytics',
        templateUrl: 'template/stats.html'
      })
      .otherwise({
        redirectTo: '/schemas'
      });
  });
