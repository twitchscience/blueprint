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
  .factory('KinesisConfig', function($resource) {
    return $resource(
      '/kinesisconfigs', null,
      {all:    {                                             method: 'GET', isArray: true},
       get:    {url: '/kinesisconfig/:account/:type/:name',  method: 'GET'},
       put:    {url: '/kinesisconfig',                       method: 'PUT'},
       update: {url: '/kinesisconfig/:account/:type/:name',  method: 'POST'},
       drop:   {url: '/drop/kinesisconfig',                  method: 'POST'},
      }
    );
  })
  .factory('Types', function($resource) {
    return $resource('/types', null, null);
  })
  .factory('Droppable', function($resource) {
    return $resource('/droppable/schema/:scope', null, null);
  })
  .factory('EventMetadata', function($resource) {
    return $resource(
      '/metadata/:scope', null,
      {get:    {url: '/metadata/:scope', method: 'GET'},
       update: {url: '/metadata/:event', method: 'POST'},
      }
    );
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
  .factory('SchemaMaintenance', function($resource) {
      return $resource(
          '/maintenance/:schema', null,
          {get:  {method:'GET', url: '/maintenance/:schema'},
            post: {method:'POST', url: '/maintenance/:schema'}
          }
      );
  })
  .factory('Stats', function($resource) {
      return $resource('/stats', null, null);
  });
