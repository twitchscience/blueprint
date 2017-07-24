describe('blueprint.kinesisconfig.create module', function() {
  var $controller, $rootScope;

  beforeEach(module('blueprint.kinesisconfig.create'));

  beforeEach(inject(function(_$controller_, _$rootScope_){
    $controller = _$controller_;
    $rootScope = _$rootScope_;
  }));

  describe('CreateKinesisConfig controller', function(){
    var controller;

    beforeEach(function() {
      controller = $controller('CreateKinesisConfig', { $scope: $rootScope });
    });

    it('is initialized correctly', inject(function() {
      expect(controller).toBeDefined();
    }));

  });
});
