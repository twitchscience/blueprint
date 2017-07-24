describe('blueprint.schema.list module', function() {
  var $controller, $rootScope;

  beforeEach(module('blueprint.schema.list'));

  beforeEach(inject(function(_$controller_, _$rootScope_){
    $controller = _$controller_;
    $rootScope = _$rootScope_;
  }));

  describe('ListSchemas controller', function(){
    var controller;

    beforeEach(function() {
      controller = $controller('ListSchemas', { $scope: $rootScope });
    });

    it('is initialized correctly', inject(function() {
      expect(controller).toBeDefined();
    }));

  });
});
