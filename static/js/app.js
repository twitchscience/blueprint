angular.module('blueprint', ['ngResource', 'ngRoute'])
  .factory('Event', function($resource) {
    return $resource(
      '/event/:scope', null,
      {all: {url: '/events/all', method: 'GET', isArray: true},
       published: {url: '/events/published', method: 'GET'},
       history: {url: '/events/:scope/history', method: 'GET'}}
    );
  })
  .factory('Schema', function($resource) {
    return $resource(
      '/schemas', null,
      {all: {method: 'GET', isArray: true},
       get: {url: '/schema/:scope', method:'GET', isArray: true},
       put: {url: '/schema', method: 'PUT'},
       update: {url: '/schema/:event', method: 'POST'},
       expire: {url: '/expire', method: 'POST'}}
    );
  })
  .factory('Types', function($resource) {
    return $resource(
      '/types', null, null
    );
  })
  .config(function($routeProvider) {
    $routeProvider
      .when('/events/all', {
        controller: 'EventListCtrl',
        templateUrl: 'template/event/list.html'
      })
      .when('/event/:scope', {
        controller: 'EventCtrl',
        templateUrl: 'template/event/show.html'
      })
      .when('/schemas', {
        controller: 'SchemaListCtrl',
        templateUrl: 'template/schema/list.html'
      })
      .when('/schema', {
        controller: 'SchemaCreateCtrl',
        templateUrl: 'template/schema/create.html'
      })
      .when('/schema/:scope', {
        controller: 'SchemaShowCtrl',
        templateUrl: 'template/schema/show.html'
      })
      .when('/cache/expire', {
        controller: 'SchemaCacheExpireCtrl',
        templateUrl: 'template/noop.html'
      })
      .otherwise({
        redirectTo: '/schemas'
      });
  })
  .controller('EventListCtrl', function($scope, Event) {
    Event.all(function(data) {
      $scope.events = data;
    });
  })
  .controller('EventCtrl', function($scope, $routeParams, $location, $q, store, Schema, Event) {
    var event, schema, types;
    var eventData = Event.get($routeParams, function(data) {
      if (data[0]) {
        event = data[0];
        angular.forEach(event.properties, function(prop) {
          if (prop.freq > 60 && prop.name !== 'token') {
            prop.publish = true;
          }
        });
      }
    }).$promise;

    var schemaData = Schema.get($routeParams, function(data) {
      if (data[0]) {
        schema = data[0];
      }
    }).$promise;

    $q.all([eventData, schemaData]).then(function() {
      // ordering is important here; we want to send you to the
      // schema page if it exists, otherwise to the event page to
      // begin creating the schema
      // TODO: angular tests so that things like this don't need comments!
      if (schema) {
        $location.path('/schema/' + schema.eventname);
      } else if (event) {
        $scope.event = event;
        $scope.showCreateSchema = function() {
          store.setEvent($scope.event);
          $location.path('/schema/create');
        };
      } else {
        store.setError('no event or schema by this name', '/');
      }
    });
  })
  .controller('SchemaCacheExpireCtrl', function($location, Schema) {
    Schema.expire(function(data) {
      $location.path('/');
    });
  })
  .controller('SchemaShowCtrl', function ($scope, $routeParams, $q, store, Schema, Types) {
    var types, schema;
    var typeRequest = Types.get(function(data) {
      if (data) {
        types = data.result;
      } else {
        // We may want a different error behavior than what the store
        // error offers. For example, here we probably want a error div
        // to populate, but for the creation app to still function.
        types = [];
      }
    }).$promise;

    var schemaRequest = Schema.get($routeParams, function(data) {
      if (data) {
        schema = data[0];
      }
    }).$promise;

    $q.all([typeRequest, schemaRequest]).then(function() {
      if (!schema || !types) {
        store.setError('API Error', '/schemas');
      }
      $scope.schema = schema;
      $scope.additions = {Columns: [], EventName: schema.EventName}; // Used to hold new columns
      $scope.types = types;

      // TODO: dry this up, it is repeated four times in this file
      $scope.newCol = {
        InboundName: '',
        OutboundName: '',
        Transformer: '',
        size: 255,
        ColumnCreationOptions: ''
      };
      $scope.addColumnToSchema = function(column) {
        if (!column.InboundName || !column.OutboundName || !column.Transformer) {
          return false;
        }
        if (column.Transformer === 'varchar') {
          if (parseInt(column.size)) {
            column.ColumnCreationOptions = '(' + parseInt(column.size) + ')';
          } else {
            return false;
          }
        }

        // Update the view, but we only submit $scope.additions
        $scope.additions.Columns.push(column);
        $scope.newCol = {
          InboundName: '',
          OutboundName: '',
          Transformer: '',
          size: 255,
          ColumnCreationOptions: ''
        };
      };
      $scope.updateSchema = function() {
        var additions = $scope.additions;
        if (additions.Columns.length < 1) {
          return false;
        }
        Schema.update({event: additions.EventName}, additions, function() {
          $location.path('/schema/' + additions.EventName);
        });
      };
    });
  })
  .controller('SchemaListCtrl', function($scope, $location, Schema) {
    Schema.all(function(data) {
      $scope.schemas = data;
    });
  })
  .controller('SchemaCreateCtrl', function($scope, $location, $q, store, Schema, Types) {
    var types;
    var typeData = Types.get(function(data) {
      if (data) {
        types = data.result;
      } else {
        // We may want a different error behavior than what the store
        // error offers. For example, here we probably want a error div
        // to populate, but for the creation app to still function.
        types = [];
      }
    }).$promise;

    $q.all([typeData]).then(function() {
      var event = {};
      // this is icky, it is tightly coupled to what spade is
      // looking for. It would be good to have an intermediate
      // representation which BluePrint converts to what spade cares
      // about but for the timebeing this is the quickest solution
      var columns = [{
        InboundName: 'time',
        OutboundName: 'time',
        Transformer: 'f@timestamp@unix',
        ColumnCreationOptions: ' sortkey'
      },{
        InboundName: 'ip',
        OutboundName: 'ip',
        Transformer: 'varchar',
        size: 15,
        ColumnCreationOptions: ''
      },{
        InboundName: 'ip',
        OutboundName: 'city',
        Transformer: 'ipCity',
        ColumnCreationOptions: ''
      },{
        InboundName: 'ip',
        OutboundName: 'country',
        Transformer: 'ipCountry',
        ColumnCreationOptions: ''
      },{
        InboundName: 'ip',
        OutboundName: 'region',
        Transformer: 'ipRegion',
        ColumnCreationOptions: ''
      },{
        InboundName: 'ip',
        OutboundName: 'asn',
        Transformer: 'ipAsn',
        ColumnCreationOptions: ''
      }];
      // TODO: Prepopulate with properties from event sampling
      /*angular.forEach(event.properties, function(v, k) {
        if (v.publish) {
          properties.push({
            InboundName: v.name,
            OutboundName: v.name,
            Transformer: v.type,
            size: v.size,
            ColumnCreationOptions: ''
          });
        }
      });*/

      event.Columns = columns;
      event.distkey = '';
      $scope.event = event;
      $scope.types = types;
      // This should have a prototype.
      $scope.newCol = {
        InboundName: '',
        OutboundName: '',
        Transformer: '',
        size: 0,
        ColumnCreationOptions: ''
      };
      $scope.addColumnToSchema = function(column) {
        $scope.event.Columns.push(column);
        $scope.newCol = {
          InboundName: '',
          OutboundName: '',
          Transformer: '',
          size: 0,
          ColumnCreationOptions: ''
        };
      };
      $scope.createSchema = function() {
        var setDistKey = $scope.event.distkey;
        angular.forEach($scope.event.Columns, function(item) {
          if (item.size) {
            item.ColumnCreationOptions += '(' + item.size + ')';
          }
          if (setDistKey == item.OutboundName) {
            item.ColumnCreationOptions += ' distkey';
          }
          if (item.Transformer === 'int') {
            item.Transformer = 'bigint';
          }
          delete item.size;
        });
        delete $scope.event.distkey;
        Schema.put($scope.event, function() {
          $location.path('/schema/' + $scope.event.EventName);
        }, function(err) {
          // TODO: handle errors correctly
          store.setError(err.data.error, '/schemas');
          return;
        });
      };
    });
  })
  .service('store', function($location) {
    var data = {
      event: undefined,
      error: undefined
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
        $location.path(path);
      },

      // TODO: use
      getError: function() {
        var t = data.error;
        data.error = undefined;
        return t;
      }
    };
  });
