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
      $scope = {};
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

    it('accepts valid examples of a Kinesis configuration', inject(function() {
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
    }));

    it('rejects Kinesis configurations with invalid JSON', inject(function() {
      testCases = [
        {
          // JSON is missing the closing bracket
          config: '{"StreamName": "test-stream", "StreamType": "stream"',
        },
        {
          // JSON is empty
          config: '',
        },
      ];
      var expectedErrorMsg = "Invalid JSON - could not be parsed";
      for (var i = 0; i < testCases.length; i++) {
        $scope.configJSON = testCases[i].config;
        $scope.AWSAccount = testCases[i].account;
        
        storeMock.clearData();
        $scope.createKinesisConfig();
        expect(storeMock.getData().error).toContain(expectedErrorMsg);
        expect(storeMock.getData().message).toBeUndefined();
        expect(kinesisMock.getData().params).toBeUndefined();
      }
    }));

    it('rejects Kinesis configurations missing required fields', inject(function() {
      testCases = [
        {
          // Missing AWS account
          config: '{"StreamName": "test-stream", "StreamType": "stream"}',
          account: 0,
        },
        {
          // JSON is missing StreamName
          config: '{"StreamType": "stream"}',
          account: 100000000001,
        },
        {
          // JSON is an empty dictionary
          config: '{}',
          account: 100000000001,
        },
      ];
      var expectedErrorMsg = "AWS account, stream name and stream type must be present";
      for (var i = 0; i < testCases.length; i++) {
        $scope.configJSON = testCases[i].config;
        $scope.AWSAccount = testCases[i].account;
        
        storeMock.clearData();
        $scope.createKinesisConfig();
        expect(storeMock.getData().error).toEqual(expectedErrorMsg);
        expect(storeMock.getData().message).toBeUndefined();
        expect(kinesisMock.getData().params).toBeUndefined();
      }
    }));

    it('handles success/failure callbacks well', inject(function() {
      var testCases = [
        {
          callback: $scope.successCallback,
          arg: {'StreamName': 'test-stream'},
          expectFailure: false,
          expectedMsg: 'Successfully created Kinesis config: test-stream',
          expectedLocation: '/kinesisconfigs',
        },
        {
          callback: $scope.failureCallback,
          arg: {data: ''},
          expectFailure: true,
          expectedMsg: 'Error creating Kinesis Config:',
          expectedLocation: '/',
        },
        {
          callback: $scope.failureCallback,
          arg: {data: 'Expected error msg'},
          expectFailure: true,
          expectedMsg: 'Expected error msg',
          expectedLocation: '/',
        },
      ];
      for (var i = 0; i < testCases.length; i++) {
        storeMock.clearData();
        $location.path('/');
        testCases[i].callback(testCases[i].arg);
        
        if (testCases[i].expectFailure) {
          expect(storeMock.getData().error).toContain(testCases[i].expectedMsg);
          expect(storeMock.getData().message).toBeUndefined();
        } else {
          expect(storeMock.getData().error).toBeUndefined();
          expect(storeMock.getData().message).toContain(testCases[i].expectedMsg);
        }
        expect($location.path()).toEqual(testCases[i].expectedLocation);
      }
    }));
  });
});
