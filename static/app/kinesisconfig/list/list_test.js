describe('blueprint.kinesisconfig.list module', function() {
  var $controller, $rootScope;

  beforeEach(module('blueprint.kinesisconfig.list'));

  beforeEach(inject(function(_$controller_, _$rootScope_){
    $controller = _$controller_;
    $rootScope = _$rootScope_;
  }));

  describe('ListKinesisConfigs controller', function(){
    var controller;

    beforeEach(function() {
      controller = $controller('ListKinesisConfigs', { $scope: $rootScope });
    });

    it('is initialized correctly', inject(function() {
      expect(controller).toBeDefined();
    }));

  });
});
