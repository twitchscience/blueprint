describe('blueprint.stats module', function() {
  var $controller, $rootScope;

  beforeEach(module('blueprint.stats'));

  beforeEach(inject(function(_$controller_, _$rootScope_){
    $controller = _$controller_;
    $rootScope = _$rootScope_;
  }));

  describe('Analytics controller', function(){
    var controller;

    beforeEach(function() {
      controller = $controller('Analytics', { $scope: $rootScope });
    });

    it('is initialized correctly', inject(function() {
      expect(controller).toBeDefined();
    }));

  });
});
