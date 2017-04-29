angular.module('blueprint')
  .controller('ShowSchema', function ($scope, $http, $location, $routeParams, $q, store, Schema, Types, Droppable, Column, auth) {
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
    $scope.loginName = auth.getLoginName();
    auth.isEditable($scope);

    $scope.forceLoadTable = function(schema){
      $http.post("/force_load", {Table:schema.EventName}, {timeout: 7000}).success(function(data, status){
          store.setMessage("Force load successful");
      }).error(function(data,status){
          store.setError("Force load failed");
      });
    }

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
      $scope.nameMap = {}; // Used to hold renamed outbound names {originalName: newName, ...}
      angular.forEach($scope.schema.Columns, function(col, i){
        $scope.nameMap[col.OutboundName] = col.OutboundName;
      });
      $scope.types = types;
      $scope.newCol = Column.make();
      $scope.usingMappingTransformer = Column.usingMappingTransformer;
      $scope.validInboundNames = function() {
        var inboundNames = {};
        var delNames = {};
        var allColumns = $scope.schema.Columns.slice().concat($scope.additions.Columns);
        angular.forEach($scope.deletes.ColInds, function(colIndex) {
          delNames[$scope.schema.Columns[colIndex].OutboundName] = true;
        });
        angular.forEach(allColumns, function(col){
          if (!(col.OutboundName in delNames)) {
            inboundNames[col.InboundName] = true;
          }
        });
        return Object.keys(inboundNames);
      };
      $scope.addColumnToSchema = function(column) {
        if (!Column.validate(column)) {
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
        } else if (Column.usingMappingTransformer(column)) {
          if (column.mappingColumn) {
            column.SupportingColumns = column.mappingColumn;
          } else {
            store.setError("New column is invalid (needs nonempty mapping column)", undefined);
            return false;
          }
        }

        // Update the view, but we only submit $scope.additions
        $scope.additions.Columns.push(column);
        $scope.newCol = Column.make();
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
        var inboundNames = $scope.validInboundNames();
        angular.forEach($scope.deletes.ColInds, function(colIndex) {
          deletes.push($scope.schema.Columns[colIndex].OutboundName);
          delNames[$scope.schema.Columns[colIndex].OutboundName] = true;
        });

        // Check that columns which are not going to be deleted still have valid supporting columns
        if (!$scope.schema.Columns.every(function (col) {
          if (!(col.OutboundName in delNames) && col.SupportingColumns && inboundNames.indexOf(col.SupportingColumns) == -1) {
            store.setError("Can't have a column using a mapping that is going to be deleted. Offending name: " + col.OutboundName);
            return false;
          }
          return true;
        })) {
          return false;
        }

        // For columns which are not going to be deleted, check that there are no duplicates in new
        // outbound names of columns
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

        // Check that we're not adding a column with a duplicate outbound name and is using a
        // valid supporting column
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
          if (col.SupportingColumns && inboundNames.indexOf(col.SupportingColumns) == -1) {
            store.setError("Can't add a column using a mapping that was or is going to be deleted. Offending name: " + col.OutboundName);
            return false;
          }
          return true;
        })) {
          return false;
        }

        // Check that none of the renamed columns have a conflict with a new or old outbound name
        // of columns which are already in the schema
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

        // If the user is in the middle of adding a column, stop here to force a conclusion there
        if ($scope.newCol.InboundName || $scope.newCol.OutboundName) {
          store.setError("Column addition not finished. Hit \"Add!\" or clear the inbound and outbound name.");
          return false;
        }

        // Nothing was modified, so stop here
        if (additions.Columns.length + deletes.length + Object.keys(renames).length < 1) {
          store.setError("No change to columns, so no action taken.", undefined);
          return false;
        }

        // We verified that we have valid things to do, so proceed with update!
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
  });
