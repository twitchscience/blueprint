angular.module('blueprint')
  .factory('Event', function($resource) {
    return $resource(
      '/event/:scope', null,
      {all:       {url: '/events/all',            method: 'GET', isArray: true},
       published: {url: '/events/published',      method: 'GET'},
       history:   {url: '/events/:scope/history', method: 'GET'}
      }
    );
  })
  .factory('Schema', function($resource) {
    return $resource(
      '/schemas', null,
      {all:    {                       method: 'GET', isArray: true},
       get:    {url: '/schema/:scope', method: 'GET', isArray: true},
       put:    {url: '/schema',        method: 'PUT'},
       update: {url: '/schema/:event', method: 'POST'},
       drop:   {url: '/drop/schema',   method: 'POST'},
      }
    );
  })
  .factory('Types', function($resource) {
    return $resource('/types', null, null);
  })
  .factory('Droppable', function($resource) {
    return $resource('/droppable/schema/:scope', null, null);
  })
  .factory('Suggestions', function($resource) {
    return $resource(
      '/suggestions', null,
      {all: {method: 'GET', isArray: true},
       get: {
         url: '/suggestion/:scope.json',
         method:'GET',
         interceptor: {responseError: function(response) { return false; }}}
       }
    );
  })
  .factory('Maintenance', function($resource) {
      return $resource(
          '/maintenance', null,
          {get:  {method:'GET'},
           post: {method:'POST'},
          }
      );
  })
  .factory('Stats', function($resource) {
      return $resource('/stats', null, null);
  });
