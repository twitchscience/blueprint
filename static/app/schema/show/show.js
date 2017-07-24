angular.module('blueprint.schema.show', [
  'ngRoute',
  'ng-showdown',
  'blueprint.components.auth',
  'blueprint.components.column',
  'blueprint.components.rest',
  'blueprint.components.store',
]).controller('ShowSchema', function ($scope, $http, $sce, $showdown, $location, $routeParams, $q, Store, Schema, Types, Droppable, EventMetadata, SchemaMaintenance, Column, Auth) {
    var types, schema, dropMessage, cancelDropMessage, rawEventMetadata;
    var typeRequest = Types.get(function(data) {
      if (data) {
        types = data.result;
      } else {
        Store.setError('Failed to fetch type information', undefined)
        types = [];
      }
    }).$promise;
    $scope.isAdmin = Auth.isAdmin();
    $scope.eventName = $routeParams.scope;
    $scope.loading = true;
    $scope.loginName = Auth.getLoginName();
    $scope.eventMetadata = {
      "edge_type": {"metadataType": "edge_type", "editable": false, "value": "", "savedValue": ""},
      "comment": {"metadataType": "comment", "editable": false, "value": "", "savedValue": "",
                  "previewMode": false, "displayedValue": "", "previewValue": "", "collapsed": true}
    };
    $scope.toggleSchemaMaintenanceMode = function() {
      if (!$scope.toggleSchemaMaintenanceModeReason) {
        Store.setError("Please enter a reason for turning schema maintenance mode " + $scope.schemaMaintenanceDirection);
        return
      }
      $scope.togglingSchemaMaintenanceMode = true;
      SchemaMaintenance.post({schema: $scope.schema.EventName},
        {is_maintenance: $scope.schemaIsEditable,
         reason: $scope.toggleSchemaMaintenanceModeReason},
        function() {
          Store.setMessage("Schema Maintenance Mode Turned " + $scope.schemaMaintenanceDirection);
          $scope.schemaIsEditable = !$scope.schemaIsEditable;
          $scope.schemaMaintenanceDirection = $scope.schemaIsEditable ? "On" : "Off";
          $scope.showSchemaMaintenance = false;
          $scope.togglingSchemaMaintenanceMode = false;
        },
        function(err) {
          Store.setError(err, undefined);
          $scope.showSchemaMaintenance = false;
          $scope.togglingSchemaMaintenanceMode = false;
        });
    };
    Auth.globalIsEditable($scope);

    $scope.forceLoadTable = function(schema){
      $http.post("/force_load", {Table:schema.EventName}, {timeout: 7000}).success(function(data, status){
          Store.setMessage("Force load successful");
      }).error(function(data,status){
          Store.setError("Force load failed, try again in a couple of minutes. If the problem persists, please report in #scieng.");
      });
    }

    $scope.setEventMetadata = function(data) {
      Object.keys(data.Metadata).forEach(function(metadataType) {
          $scope.eventMetadata[metadataType].metadataType = metadataType;
          $scope.eventMetadata[metadataType].value = data.Metadata[metadataType].MetadataValue;
          $scope.eventMetadata[metadataType].savedValue = data.Metadata[metadataType].MetadataValue;
          if (metadataType == "comment") {
            $scope.eventMetadata[metadataType].displayedValue = data.Metadata[metadataType].MetadataValue;
          }
      })
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
      Store.setError(msg, '/schemas');
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
      Store.setError(msg);
    }).$promise;

    var eventMetadataRequest = EventMetadata.get($routeParams, function(data) {
      if (data) {
        rawEventMetadata = data;
      } else {
        Store.setError('Failed to fetch event metadata');
      }
    }, function(err) {
      var msg;
      if (err.data) {
        msg = 'API Error: ' + err.data;
      } else {
        msg = 'Schema not found or threw an error when retrieving event metadata';
      }
      Store.setError(msg);
    }).$promise;

    var schemaMaintenanceRequest = SchemaMaintenance.get({schema: $routeParams['scope']}, function(data) {
      if (data) {
        schemaIsEditable = !data['is_maintenance'];
        schemaMaintenanceModeUser = data['user'];
      } else {
        Store.setError('Failed to fetch schema maintenance status');
      }
    }, function(err) {
      var msg;
      if (err.data) {
        msg = 'API Error: ' + err.data;
      } else {
        msg = 'threw an error when retrieving schema maintenance status';
      }
      Store.setError(msg);
    }).$promise;


    $q.all([typeRequest, schemaRequest, droppableRequest, eventMetadataRequest, schemaMaintenanceRequest]).then(function() {
      if (!schema || !types) {
        Store.setError('API Error', '/schemas');
      }
      $scope.loading = false;
      $scope.showDropTable = false;
      $scope.dropTableReason = '';
      $scope.dropMessage = dropMessage;
      $scope.executingDrop = false;
      $scope.cancelDropMessage = cancelDropMessage;
      $scope.successDropMessage = successDropMessage;
      $scope.setEventMetadata(rawEventMetadata);
      $scope.schemaIsEditable = schemaIsEditable;
      $scope.schemaMaintenanceModeUser = schemaMaintenanceModeUser;
      $scope.schemaMaintenanceDirection = $scope.schemaIsEditable ? "On" : "Off";
      $scope.showSchemaMaintenance = false;
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
          Store.setError("New column is invalid", undefined);
          return false;
        }
        Store.clearError();
        if (column.Transformer === 'varchar') {
          if (parseInt(column.size)) {
            column.ColumnCreationOptions = '(' + parseInt(column.size) + ')';
          } else {
            Store.setError("New column is invalid (needs nonempty value)", undefined);
            return false;
          }
        } else if (Column.usingMappingTransformer(column)) {
          if (column.mappingColumn) {
            column.SupportingColumns = column.mappingColumn;
          } else {
            Store.setError("New column is invalid (needs nonempty mapping column)", undefined);
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
      $scope.togglePreviewEventComment = function() {
        var comment = $scope.eventMetadata.comment;
        comment.previewMode = !comment.previewMode;
        comment.previewValue = comment.value;
      }
      $scope.cancelEditEventMetadata = function(metadataType) {
        if (metadataType == "comment") {
          $scope.eventMetadata[metadataType].previewMode = false;
        }
        $scope.eventMetadata[metadataType].editable = false;
        // Reset event metadata to saved version
        $scope.eventMetadata[metadataType].value = $scope.eventMetadata[metadataType].savedValue;
      }
      $scope.editEventMetadata = function(metadataType) {
        $scope.eventMetadata[metadataType].editable = true;
      }
      $scope.updateEventMetadata = function(metadataType) {
        var metadataRow = $scope.eventMetadata[metadataType];
        metadataRow.editable = false;

        if (metadataRow.value != metadataRow.savedValue) {
          EventMetadata.update(
            {event: $scope.schema.EventName},
            {MetadataType: metadataType,
             MetadataValue: metadataRow.value
            },
            function() {
              Store.setMessage("Successfully updated " + metadataType + " for " +  schema.EventName);
              metadataRow.savedValue = metadataRow.value;
              metadataRow.editable = false;
              if (metadataType == "comment") {
                metadataRow.displayedValue = metadataRow.value;
                metadataRow.previewMode = false;
              }
            },
            function(err) {
              Store.setError(err);
              metadataRow.editable = true;
            });
        }
      };
      $scope.blacklistedOutboundNames = ["date"];
      $scope.updateSchema = function() {
        var additions = $scope.additions;
        var deletes = [];
        var delNames = {};
        var inboundNames = $scope.validInboundNames();
        angular.forEach($scope.deletes.ColInds, function(colIndex) {
          deletes.push($scope.schema.Columns[colIndex].OutboundName);
          delNames[$scope.schema.Columns[colIndex].OutboundName] = true;
        });


        // Check that time isn't being deleted or renamed away
        if (deletes.indexOf("time") != -1) {
          Store.setError("Cannot delete the time column.");
          return false;
        }
        if ("time" in $scope.nameMap && $scope.nameMap["time"] != "time") {
          Store.setError("Cannot rename the time column.");
          return false;
        }

        // Check that none of the added columns or the renames are blacklisted
        // outbound names
        if (!$scope.additions.Columns.every(function (col) {
          if ($scope.blacklistedOutboundNames.indexOf(col.OutboundName.toLowerCase()) != -1) {
            Store.setError("Cannot have an outbound name '" + col.OutboundName + "'. It is a reserved identifier.");
            return false;
          }
          return true;
        })) {
          return false;
        }
        if (!Object.keys($scope.nameMap).every(function (oldName) {
          var newName = $scope.nameMap[oldName];
          if($scope.blacklistedOutboundNames.indexOf(newName) != -1) {
            Store.setError("Cannot have an outbound name '" + newName + "'. It is a reserved identifier.");
            return false;
          }
          return true;
        })) {
          return false;
        }

        // Check that columns which are not going to be deleted still have valid supporting columns
        if (!$scope.schema.Columns.every(function (col) {
          if (!(col.OutboundName in delNames) && col.SupportingColumns && inboundNames.indexOf(col.SupportingColumns) == -1) {
            Store.setError("Can't have a column using a mapping that is going to be deleted. Offending name: " + col.OutboundName);
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
            Store.setError("Duplicate name. Offending name: " + newName);
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
            Store.setError("Duplicate name. Offending name: " + col.OutboundName);
            return false;
          }
          newNames[col.OutboundName] = true;
          if (col.OutboundName in oldNames) {
            Store.setError("Can't add a column while renaming away from it. Offending name: " + col.OutboundName);
            return false;
          }
          if (col.SupportingColumns && inboundNames.indexOf(col.SupportingColumns) == -1) {
            Store.setError("Can't add a column using a mapping that was or is going to be deleted. Offending name: " + col.OutboundName);
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
                Store.setError("Cannot rename from or to a column that was already renamed from or to. Offending name: " + newName);
                return false;
              }
              if(originalName in nameSet) {
                Store.setError("Cannot rename from or to a column that was already renamed from or to. Offending name: " + originalName);
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
          Store.setError("Column addition not finished. Hit \"Add!\" or clear the inbound and outbound name.");
          return false;
        }

        // Nothing was modified, so stop here
        if (additions.Columns.length + deletes.length + Object.keys(renames).length < 1) {
          Store.setError("No change to columns, so no action taken.", undefined);
          return false;
        }

        // We verified that we have valid things to do, so proceed with update!
        Schema.update(
          {event: schema.EventName},
          {additions: additions.Columns, deletes: deletes, renames: renames},
          function() {
            Store.setMessage("Succesfully updated schema: " +  schema.EventName);
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
            Store.setError(err, undefined);
          });
      };
      $scope.dropTable = function() {
        if ($scope.dropTableReason === '') {
          Store.setError("Please enter a reason for dropping the table");
          return false
        }
        $scope.executingDrop = true;
        Schema.drop(
          {EventName: schema.EventName, Reason: $scope.dropTableReason},
          function() {
            Store.setMessage($scope.successDropMessage);
            $location.path('/schemas');
            $scope.executingDrop = false;
          },
          function(err) {
            Store.setError(err, undefined);
            $scope.executingDrop = false;
          });
      };
    });
  });
