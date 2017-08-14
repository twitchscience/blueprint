describe('blueprint.kinesisconfig.create module', function() {
  var $controller, $scope, $location;

  beforeEach(module('blueprint.kinesisconfig.create'));

  beforeEach(inject(function(_$controller_, _$rootScope_, _$location_){
    $controller = _$controller_;
    $scope = _$rootScope_.$new();
    $location = _$location_;
  }));

  describe('CreateKinesisConfig controller', function(){
    var controller, authMock, storeMock, kinesisMock;

    var authMockGenerator = function(isAdmin){
      return {
        getLoginName: function() {
          return 'test_login';
        },
        isAdmin: function() {
          return isAdmin;
        },
      };
    };
    
    var kinesisMockGenerator = function() {
      var data = {
        params: undefined,
        successCallback: undefined,
        failureCallback: undefined,
      };
      return {
        put: function(params, success, failure) {
          data.params = params;
          data.successCallback = success;
          data.failureCallback = failure;
        },
        getData: function() {
          return data;
        },
      };
    };
    
    var storeMockGenerator = function(){
      var data = {
        message: undefined,
        error: undefined,
      };
      return {
        setError: function(err, path) {
          data.error = err;
        },
        clearError: function() {
          data.error = undefined;
        },
        setMessage: function(msg) {
          data.message = msg;
        },
        getData: function() {
          return data;
        },
        clearData: function() {
          data.error = undefined;
          data.message = undefined;
        },
      };
    };

    beforeEach(function() {
      $scope = {}
      authMock = authMockGenerator(false);
      kinesisMock = kinesisMockGenerator();
      storeMock = storeMockGenerator();

      controller = $controller('CreateKinesisConfig', { 
        $scope: $scope,
        $location: $location,
        Auth: authMock,
        KinesisConfig: kinesisMock,
        Store: storeMock,
      });
    });

    it('is initialized correctly', inject(function() {
      expect(controller).toBeDefined();
      expect($scope.loginName).toEqual('test_login');
      expect($scope.isAdmin).toBeFalsy();
      expect($scope.AWSAccount).toEqual(0);
      expect($scope.Team).toEqual('');
      expect($scope.Contact).toEqual('');
      expect($scope.Usage).toEqual('');
      expect($scope.ConsumingLibrary).toEqual('');
      expect($scope.SpadeConfig).toEqual('');
      expect($scope.configJSON).toEqual('');
    }));

    it('accepts a fully valid example of a Kinesis configuration', inject(function() {
      var jsonConfig = '{' +
      ' "StreamName": "test-stream",' +
      ' "StreamRole": "arn:aws:iam::100000000001:role/test-stream",' +
      ' "StreamType": "stream",' +
      ' "StreamRegion": "us-west-2",' +
      ' "Compress": false,' +
      ' "FirehoseRedshiftStream": false,' +
      ' "EventNameTargetField": "",' +
      ' "ExcludeEmptyFields": false,' +
      ' "BufferSize": 1024,' +
      ' "MaxAttemptsPerRecord": 10,' +
      ' "RetryDelay": "1s",' +
      ' "Events": {' +
      '   "test_event": {' +
      '     "Filter": "",' +
      '     "Fields": [' +
      '       "field_1",' +
      '       "field_2"' +
      '     ],' +
      '     "FieldRenames": {}' +
      '   }' +
      ' },' +
      ' "Globber": {' +
      '   "MaxSize": 990000,' +
      '   "MaxAge": "1s",' +
      '   "BufferLength": 1024' +
      ' },' +
      ' "Batcher": {' +
      '   "MaxSize": 990000,' +
      '   "MaxEntries": 500,' +
      '   "MaxAge": "1s",' +
      '   "BufferLength": 1024' +
      ' }' +
      '}';

      $scope.configJSON = jsonConfig;
      $scope.AWSAccount = 100000000001;
      $scope.Team = 'test_team';
      $scope.Contact = 'test_contact';
      $scope.Usage = 'testing';
      $scope.ConsumingLibrary = 'kinsumer';

      $scope.createKinesisConfig();
      expect(storeMock.getData().error).toBeUndefined();
      expect(storeMock.getData().message).toBeUndefined();

      var expectedParams = {
        'StreamName': 'test-stream',
        "StreamType": 'stream',
        'AWSAccount': 100000000001,
        'Team': 'test_team',
        'Contact': 'test_contact',
        'Usage': 'testing',
        'ConsumingLibrary': 'kinsumer',
        'SpadeConfig': JSON.parse(jsonConfig),
      };
      expect(kinesisMock.getData().params).toEqual(expectedParams);
      expect(kinesisMock.getData().successCallback).toBeDefined();
      expect(kinesisMock.getData().failureCallback).toBeDefined();

      storeMock.clearData();
      kinesisMock.getData().successCallback();
      expect(storeMock.getData().error).toBeUndefined();
      expect(storeMock.getData().message).toEqual("Successfully created Kinesis config: test-stream");
    }));

    it('rejects a Kinesis configuration with invalid JSON', inject(function() {
      // JSON is missing the closing bracket
      $scope.configJSON = '{"StreamName": "test-stream", "StreamType": "stream"';

      $scope.createKinesisConfig();
      expect(storeMock.getData().error).toContain("Invalid JSON - could not be parsed");
      expect(storeMock.getData().message).toBeUndefined();
      expect(kinesisMock.getData().params).toBeUndefined();

      // JSON is empty
      $scope.configJSON = '';

      storeMock.clearData();
      $scope.createKinesisConfig();
      expect(storeMock.getData().error).toContain("Invalid JSON - could not be parsed");
      expect(storeMock.getData().message).toBeUndefined();
      expect(kinesisMock.getData().params).toBeUndefined();
    }));

    it('rejects a Kinesis configurations missing required fields', inject(function() {
      // Missing AWS account
      $scope.configJSON = '{"StreamName": "test-stream", "StreamType": "stream"}';

      $scope.createKinesisConfig();
      expect(storeMock.getData().error).toEqual("AWS account, stream name and stream type must be present");
      expect(storeMock.getData().message).toBeUndefined();
      expect(kinesisMock.getData().params).toBeUndefined()

      // JSON is missing StreamName
      $scope.configJSON = '{"StreamType": "stream"}';
      $scope.AWSAccount = 100000000001;

      storeMock.clearData();
      $scope.createKinesisConfig();
      expect(storeMock.getData().error).toEqual("AWS account, stream name and stream type must be present");
      expect(storeMock.getData().message).toBeUndefined();
      expect(kinesisMock.getData().params).toBeUndefined()

      // JSON is an empty dictionary
      $scope.configJSON = '{}';
      $scope.AWSAccount = 100000000001;

      storeMock.clearData();
      $scope.createKinesisConfig()
      expect(storeMock.getData().error).toEqual("AWS account, stream name and stream type must be present");
      expect(storeMock.getData().message).toBeUndefined();
      expect(kinesisMock.getData().params).toBeUndefined()
    }));

    it('handles PUT request failures', inject(function() {
      var jsonConfig = '{"StreamName": "test-stream","StreamType": "stream"}';
      $scope.configJSON = jsonConfig;
      $scope.AWSAccount = 100000000001;
      $scope.Team = 'test_team';
      $scope.Contact = 'test_contact';
      $scope.Usage = 'testing';
      $scope.ConsumingLibrary = 'kinsumer';

      $scope.createKinesisConfig();
      expect(storeMock.getData().error).toBeUndefined();
      expect(storeMock.getData().message).toBeUndefined();

      expectedParams = {
        'StreamName': 'test-stream',
        "StreamType": 'stream',
        'AWSAccount': 100000000001,
        'Team': $scope.Team,
        'Contact': $scope.Contact,
        'Usage': $scope.Usage,
        'ConsumingLibrary': $scope.ConsumingLibrary,
        'SpadeConfig': JSON.parse(jsonConfig),
      }
      expect(kinesisMock.getData().params).toEqual(expectedParams)
      expect(kinesisMock.getData().successCallback).toBeDefined()
      expect(kinesisMock.getData().failureCallback).toBeDefined()

      storeMock.clearData();
      kinesisMock.getData().failureCallback({data: ''});
      expect(storeMock.getData().error).toContain('Error creating Kinesis Config');
      expect(storeMock.getData().message).toBeUndefined();

      storeMock.clearData();
      kinesisMock.getData().failureCallback({data: 'Expected error msg'});
      expect(storeMock.getData().error).toContain('Expected error msg');
      expect(storeMock.getData().message).toBeUndefined();
    }));
  });
});
