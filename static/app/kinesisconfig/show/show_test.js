describe('blueprint.kinesisconfig.show module', function() {
  var $controller, $rootScope;

  beforeEach(module('blueprint.kinesisconfig.show'));

  beforeEach(inject(function(_$controller_, _$rootScope_){
    $controller = _$controller_;
    $rootScope = _$rootScope_;
  }));

  describe('ShowKinesisConfig controller', function(){
    var controller;

    beforeEach(function() {
      controller = $controller('ShowKinesisConfig', { $scope: $rootScope });
    });

    it('is initialized correctly', inject(function() {
      expect(controller).toBeDefined();
    }));

  });
});
