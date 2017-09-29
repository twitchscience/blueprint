describe('blueprint.schema.show module', function() {
  var $controller, $rootScope;

  beforeEach(module('blueprint.schema.show'));

  beforeEach(inject(function(_$controller_, _$rootScope_){
    $controller = _$controller_;
    $rootScope = _$rootScope_;
  }));

  describe('ShowSchema controller', function(){
    var controller;

    beforeEach(function() {
      controller = $controller('ShowSchema', { $scope: $rootScope });
    });

    it('is initialized correctly', inject(function() {
      expect(controller).toBeDefined();
    }));
    it('Loads metadata without error', inject(function() {
      var $scope = {};
      var metadataBody = {"EventName": "asdf",
        "Metadata": {
          "birth":
            {"MetadataValue": "2017-09-29T00:25:17+0000","TS": "2017-09-29T00:25:17.76615Z","UserName": "unknown","Version": 1},
          "datastores":
            {"MetadataValue": "ace","TS": "2017-09-29T00:25:17.850891Z","UserName": "unknown","Version": 1}}};
      controller = $controller('ShowSchema', { $scope: $scope });
      $scope.setEventMetadata(metadataBody);
      expect($scope.eventMetadata).toBeDefined();
    }));
  });
});
