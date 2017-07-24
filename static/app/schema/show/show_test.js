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

  });
});
