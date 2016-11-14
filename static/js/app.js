angular.module('blueprint', ['ngResource', 'ngRoute', 'ngCookies'])
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
       drop: {url: '/drop/schema', method: 'POST'},
      }
    );
  })
  .factory('Types', function($resource) {
    return $resource(
      '/types', null, null
    );
  })
  .factory('Droppable', function($resource) {
    return $resource(
      '/droppable/schema/:scope', null, null
    );
  })
  .factory('Suggestions', function($resource) {
    return $resource(
      '/suggestions', null,
      {all: {method: 'GET', isArray: true},
       get: {url: '/suggestion/:scope.json', method:'GET',
             interceptor: {responseError: function(response) {return false;}}}
       }
    );
  })
  .factory('ColumnMaker', function() {
    return {
      make: function() {
      return {
        InboundName: '',
        OutboundName: '',
        Transformer: 'varchar',
        size: 255,
        ColumnCreationOptions: ''
        };
      },
      validate: function(column) {
        if (!column.InboundName || !column.OutboundName || !column.Transformer) {
          return false;
        } else if (column.Transformer == 'varchar' && !(column.size > 0 && column.size <= 65535)) {
          return false;
        }
        return true;
      }
    }
  })
  .config(function($routeProvider) {
    $routeProvider
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
      .otherwise({
        redirectTo: '/schemas'
      });
  })
  .controller('HeaderCtrl', function($scope, store, auth) {
    $scope.getError = store.getError;
    $scope.clearError = store.clearError;
    $scope.getMessage = store.getMessage;
    $scope.clearMessage = store.clearMessage;
    $scope.loginName = auth.getLoginName();
  })
  .controller('SchemaShowCtrl', function ($scope, $location, $routeParams, $q, store, Schema, Types, Droppable, ColumnMaker) {
    var types, schema, dropMessage, cancelDropMessage;
    var typeRequest = Types.get(function(data) {
      if (data) {
        types = data.result;
      } else {
        store.setError('Failed to fetch type information', undefined)
        types = [];
      }
    }).$promise;
    $scope.eventName = $routeParams.scope;
    $scope.loading = true;

    var schemaRequest = Schema.get($routeParams, function(data) {
      if (data) {
        schema = data[0];
      }
    }, function(err) {
      var msg;
      if (err.data) {
        msg = 'API Error: ' + err.data;
      } else {
        msg = 'Schema not found or threw an error';
      }
      store.setError(msg, '/schemas');
    }).$promise;

    function makeUndroppable() {
      dropMessage = 'Request Table Drop';
      cancelDropMessage = 'Cancel Drop Request';
      successDropMessage = 'Requested Table Drop';
    }
    var droppableRequest = Droppable.get($routeParams, function(data) {
      if (data) {
        if (data['Droppable']) {
          dropMessage = 'Drop Table';
          cancelDropMessage = 'Cancel Drop';
          successDropMessage = 'Table Dropped';
        } else {
          makeUndroppable();
        }
      }
    }, function(err) {
      var msg;
      if (err.data) {
        msg = 'API Error: ' + err.data;
      } else {
        msg = 'Schema not found or threw an error when determining if droppable';
      }
      makeUndroppable()
      store.setError(msg);
    }).$promise;

    $q.all([typeRequest, schemaRequest, droppableRequest]).then(function() {
      if (!schema || !types) {
        store.setError('API Error', '/schemas');
      }
      $scope.loading = false;
      $scope.showDropTable = false;
      $scope.dropTableReason = '';
      $scope.dropMessage = dropMessage;
      $scope.executingDrop = false;
      $scope.cancelDropMessage = cancelDropMessage;
      $scope.successDropMessage = successDropMessage;
      $scope.schema = schema;
      $scope.additions = {Columns: []}; // Used to hold new columns
      $scope.deletes = {ColInds: []}; // Used to hold dropped columns
      $scope.nameMap = {}; // Used to hold renamed columns {originalName: newName, ...}
      angular.forEach($scope.schema.Columns, function(col, i){
        $scope.nameMap[col.OutboundName] = col.OutboundName;
      });
      $scope.types = types;
      $scope.newCol = ColumnMaker.make();
      $scope.addColumnToSchema = function(column) {
        if (!ColumnMaker.validate(column)) {
          store.setError("New column is invalid", undefined);
          return false;
        }
        store.clearError();
        if (column.Transformer === 'varchar') {
          if (parseInt(column.size)) {
            column.ColumnCreationOptions = '(' + parseInt(column.size) + ')';
          } else {
            store.setError("New column is invalid (needs nonempty value)", undefined);
            return false;
          }
        }

        // Update the view, but we only submit $scope.additions
        $scope.additions.Columns.push(column);
        $scope.newCol = ColumnMaker.make();
        document.getElementById('newInboundName').focus()
      };
      $scope.columnAlreadyStagedForDelete = function(colInd) {
        if ($scope.deletes.ColInds.indexOf(colInd) < 0) return false;
        return true;
      };
      $scope.columnIsDeletable = function(colInd) {
        forbiddenDeletes = ['distkey', 'sortkey'];
        options = $scope.schema.Columns[colInd].ColumnCreationOptions;
        for (var i = 0; i < forbiddenDeletes.length; i++) {
          if (options.indexOf(forbiddenDeletes[i]) !== -1) return false;
        }
        return true;
      };
      $scope.deleteColumnFromSchema = function(colInd) {
        $scope.deletes.ColInds.push(colInd);
      };
      $scope.undoDeleteColumnFromSchema = function(colInd) {
        undoTarget = $scope.deletes.ColInds.indexOf(colInd);
        // can only undo drop a column that was already deleted
        if (undoTarget < 0) return;
        $scope.deletes.ColInds.splice(undoTarget, 1);
      };
      $scope.dropColumnFromAdditions = function(colInd) {
        $scope.additions.Columns.splice(colInd, 1);
      };
      $scope.outboundColumnEdited = function(originalName){
        return $scope.nameMap[originalName] != originalName;
      }
      $scope.outboundColumnStyle = function(originalName){
        if($scope.outboundColumnEdited(originalName)){
          return "warning"
        }
        return ""
      }
      $scope.summaryStyle = function(num){
        if(num > 0){
          return "warning";
        }
        return "";
      }
      $scope.undoRename = function(originalName){
        $scope.nameMap[originalName] = originalName;
      }
      $scope.numRenames = function(){
        var i = 0;
        angular.forEach($scope.nameMap, function(val, originalName){
          if($scope.outboundColumnEdited(originalName)){
            i++;
          }
        });
        return i;
      }
      $scope.updateSchema = function() {
        var additions = $scope.additions;
        var deletes = [];
        var delNames = {};
        angular.forEach($scope.deletes.ColInds, function(colIndex) {
          deletes.push($scope.schema.Columns[colIndex].OutboundName);
          delNames[$scope.schema.Columns[colIndex].OutboundName] = true;
        });
        var newNames = {};
        var oldNames = {};
        if (!Object.keys($scope.nameMap).every(function (oldName) {
          var newName = $scope.nameMap[oldName];
          if (oldName in delNames) {
            return true;
          }
          oldNames[oldName] = true;
          if (newName in newNames) {
            store.setError("Duplicate name. Offending name: " + newName);
            return false;
          }
          newNames[newName] = true;
          return true;
        })) {
          return false;
        }
        if (!$scope.additions.Columns.every(function (col) {
          if (col.OutboundName in newNames) {
            store.setError("Duplicate name. Offending name: " + col.OutboundName);
            return false;
          }
          newNames[col.OutboundName] = true;
          if (col.OutboundName in oldNames) {
            store.setError("Can't add a column while renaming away from it. Offending name: " + col.OutboundName);
            return false;
          }
          return true;
        })) {
          return false;
        }
        var seenNames = {};
        if (!Object.keys(newNames).every(function(newName) {
          if (newName in seenNames) {
            store.setError("Duplicate name. Offending name: " + newName);
            return false;
          }
          seenNames[newName] = true;
          return true;
        })) {
          return false;
        }
        var renames = {};
        var nameSet = {};
        if (!Object.keys($scope.nameMap).every(function(originalName) {
              var newName = $scope.nameMap[originalName];

              if(originalName != newName){
                renames[originalName] = newName;
              }else{
                return true;
              }

              if(newName in nameSet) {
                store.setError("Cannot rename from or to a column that was already renamed from or to. Offending name: " + newName);
                return false;
              }
              if(originalName in nameSet) {
                store.setError("Cannot rename from or to a column that was already renamed from or to. Offending name: " + originalName);
                return false;
              }
              nameSet[newName] = true;
              nameSet[originalName] = true;
              return true;
        })) {
          return false;
        }
        if ($scope.newCol.InboundName || $scope.newCol.OutboundName) {
          store.setError("Column addition not finished. Hit \"Add!\" or clear the inbound and outbound name.");
          return false;
        }

        if (additions.Columns.length + deletes.length + renames.length < 1) {
          store.setError("No change to columns, so no action taken.", undefined);
          return false;
        }
        Schema.update(
          {event: schema.EventName},
          {additions: additions.Columns, deletes: deletes, renames: renames},
          function() {
            store.setMessage("Succesfully updated schema: " +  schema.EventName);
            // update front-end schema
            for (i = 0; i < $scope.deletes.ColInds.length; i++) {
              $scope.schema.Columns.splice($scope.deletes.ColInds[i], 1);
              // must decrement the indices after the delete as the column no longer exists
              for (j = i; j < $scope.deletes.ColInds.length; j++) {
                if ($scope.deletes.ColInds[j] > $scope.deletes.ColInds[i]) $scope.deletes.ColInds[j]--;
              }
            }
            $scope.deletes = {ColInds: []};
            angular.forEach($scope.additions.Columns, function(c) {
              $scope.schema.Columns.push(c);
              $scope.nameMap[c.OutboundName] = c.OutboundName
            });
            angular.forEach($scope.schema.Columns, function(c) {
              if (c.OutboundName in renames) {
                var newName = renames[c.OutboundName];
                delete $scope.nameMap[c.OutboundName];
                $scope.nameMap[newName] = newName;
                c.OutboundName = newName;
              }
            });
            $scope.additions = {Columns: []};
            $location.path('/schema/' + schema.EventName);
          },
          function(err) {
            store.setError(err, undefined);
          });
      };
      $scope.dropTable = function() {
        if ($scope.dropTableReason === '') {
          store.setError("Please enter a reason for dropping the table");
          return false
        }
        $scope.executingDrop = true;
        Schema.drop(
          {EventName: schema.EventName, Reason: $scope.dropTableReason},
          function() {
            store.setMessage($scope.successDropMessage);
            $location.path('/schemas');
            $scope.executingDrop = false;
          },
          function(err) {
            store.setError(err, undefined);
            $scope.executingDrop = false;
          });
      };
    });
  })
  .controller('SchemaListCtrl', function($scope, $location, $http, Schema, Suggestions, store, auth) {
    $scope.loginName = auth.getLoginName();
    $scope.ingestTable = function(schema){
      schema.IngestStatus = 'flushing';
      $http.post("/ingest", {Table:schema.EventName}, {timeout: 7000}).success(function(data, status){
        schema.IngestStatus = 'flushed';
      }).error(function(data,status){
        schema.IngestStatus = 'failed';
      });
    }
    $scope.loading = true;
    $scope.ready = false;
    Schema.all(function(data) {
      $scope.loading = false;
      $scope.schemas = data;
      var existingSchemas = {};
      angular.forEach($scope.schemas, function(s) {
        existingSchemas[s.EventName] = true;
        s.IngestStatus = 'default';
      });

      Suggestions.all(function(data) {
        $scope.loading = false;
        $scope.ready = true;
        $scope.suggestions = [];
        angular.forEach(data, function(s) {
          if (!existingSchemas[s.EventName]) {
            $scope.suggestions.push(s);
          }
        });
      });
    }, function(err) {
      $scope.loading = false;
      var msg;
      if (err.data) {
        msg = err.data;
      } else {
        msg = 'Error loading schemas:' + err;
      }
      store.setError(msg);
    });
  })
  .controller('SchemaCreateCtrl', function($scope, $location, $q, $routeParams, store, Schema, Types, Suggestions, ColumnMaker, auth) {
    $scope.loginName = auth.getLoginName();
    var types, suggestions, suggestionData;
    var typeData = Types.get(function(data) {
      if (data) {
        types = data.result;
      } else {
        store.setError('Failed to fetch type information', undefined)
        types = [];
      }
    }).$promise;

    if ($routeParams['scope']) {
      suggestionData = Suggestions.get($routeParams, function(data) {
        if (data) {
          suggestions = data;
        }
      }).$promise;
    } else {
      var deferScratch = $q.defer();
      deferScratch.resolve();
      suggestionData = deferScratch.promise;
    }

    var rewriteColumns = function(cols) {
      var rewrites = [
        {"Name": "app_version", "Change": [["size", 32]]},
        {"Name": "browser", "Change": [["size", 255]]},
        {"Name": "channel", "Change": [["size", 25]]},
        {"Name": "content_mode", "Change": [["size", 32]]},
        {"Name": "device_id", "Change": [["size", 32]]},
        {"Name": "domain", "Change": [["size", 255]]},
        {"Name": "game", "Change": [["size", 64]]},
        {"Name": "host_channel", "Change": [["size", 25]]},
        {"Name": "language", "Change": [["size", 8]]},
        {"Name": "login", "Change": [["size", 25]]},
        {"Name": "platform", "Change": [["size", 40]]},
        {"Name": "player", "Change": [["size", 32]]},
        {"Name": "preferred_language", "Change": [["size", 8]]},
        {"Name": "received_language", "Change": [["size", 8]]},
        {"Name": "referrer_domain", "Change": [["size", 255]]},
        {"Name": "referrer_url", "Change": [["size", 255]]},
        {"Name": "url", "Change": [["size", 255]]},
        {"Name": "user_agent", "Change": [["size", 255]]},
        {"Name": "vod_id", "Change": [["size", 16]]},
      ];

      var deletes = [
        "token",
      ];

      angular.forEach(rewrites, function (rule) {
        angular.forEach(cols, function(col) {
          if (col.InboundName == rule.Name) {
            angular.forEach(rule.Change, function(change) {
              col[change[0]] = change[1];
            })
          }
        });
      });

      angular.forEach(deletes, function (d) {
        for (i=0; i<cols.length; i++) {
          if (cols[i].InboundName == d) {
            cols.splice(i, 1);
            break;
          }
        }
      });
    };

    $q.all([typeData, suggestionData]).then(function() {
      var event = {distkey:''};
      var defaultColumns = [{
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
          OutboundName: 'asn_id',
          Transformer: 'ipAsnInteger',
          ColumnCreationOptions: ''
        }];
      // this is icky, it is tightly coupled to what spade is
      // looking for. It would be good to have an intermediate
      // representation which BluePrint converts to what spade cares
      // about but for the timebeing this is the quickest solution
      if (!suggestions) {
        event.Columns = defaultColumns;
      } else {
        event = suggestions;
        event.Columns.sort(function(a, b) {return b.OccurrenceProbability - a.OccurrenceProbability});

        for (i=0; i<event.Columns.length; i++) {
          if (event.Columns[i].InboundName == 'time') {
            event.Columns.splice(i, 1);
            break;
          }
        }

        var re = /\((\d+)\)/
        angular.forEach(event.Columns, function(col) {
          if (col.Transformer == 'varchar') {
            var match = re.exec(col.ColumnCreationOptions);
            if (match) {
              col.size = parseInt(match[1]);
            }
            col.ColumnCreationOptions = '';
          }
          if (col.InboundName == 'device_id') {
            event.distkey = 'device_id';
          }
        });

        event.Columns = defaultColumns.concat(event.Columns);
        rewriteColumns(event.Columns);
      }

      $scope.event = event;
      $scope.types = types;
      $scope.newCol = ColumnMaker.make();
      $scope.addColumnToSchema = function(column) {
        if (!ColumnMaker.validate(column)) {
          store.setError("New column is invalid", undefined);
          return false;
        }
        store.clearError();
        $scope.event.Columns.push(column);
        $scope.newCol = ColumnMaker.make();
        document.getElementById('newInboundName').focus();
      };
      $scope.dropColumnFromSchema = function(columnInd) {
        $scope.event.Columns.splice(columnInd, 1);
      }
      $scope.createSchema = function() {
        store.clearError();
        var setDistKey = $scope.event.distkey;
        var nameSet = {};
        angular.forEach($scope.event.Columns, function(item) {
          if(item.OutboundName in nameSet){
            store.setError("Cannot repeat column name. Repeated '"+item.OutboundName+"'");
            return false;
          } else {
            nameSet[item.OutboundName] = true;
          }
          if (!ColumnMaker.validate(item)) {
            store.setError("At least one column is invalid; look at '" + item.InboundName + "'", undefined);
            return false;
          }
          if (!item.ColumnCreationOptions) {
            item.ColumnCreationOptions = '';
          }
          if (item.Transformer === 'varchar') {
            item.ColumnCreationOptions = '(' + item.size + ')';
          }
          if (setDistKey == item.OutboundName) {
            item.ColumnCreationOptions += ' distkey';
          }
          if (item.Transformer === 'int') {
            item.Transformer = 'bigint';
          }
        });
        if (store.getError()) {
          return;
        }
        delete $scope.event.distkey;
        Schema.put($scope.event, function() {
          store.setMessage("Succesfully created schema: " + $scope.event.EventName)
          $location.path('/schema/' + $scope.event.EventName);
        }, function(err) {
          var msg;
          if (err.data) {
            msg = err.data;
          } else {
            msg = 'Error creating schema:' + err;
          }
          store.setError(msg, '/schemas');
          return;
        });
      };
    });
  })
  .service('auth', function($cookies) {
    var loginName = $cookies.get('displayName');
    return {
      getLoginName: function() {
        return loginName;
      },
    };
  })
  .service('store', function($location) {
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
