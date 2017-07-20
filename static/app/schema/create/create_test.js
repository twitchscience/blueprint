describe('blueprint.schema.create module', function() {
  var $controller, $rootScope;

  beforeEach(module('blueprint.schema.create'));

  beforeEach(inject(function(_$controller_, _$rootScope_){
    $controller = _$controller_;
    $rootScope = _$rootScope_;
  }));

  describe('CreateSchema controller', function(){
    var controller;

    beforeEach(function() {
      controller = $controller('CreateSchema', { $scope: $rootScope });
    });

    it('is initialized correctly', inject(function() {
      expect(controller).toBeDefined();
    }));

  });
});
