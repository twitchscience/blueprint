describe('blueprint.components.auth module', function() {
  beforeEach(module('blueprint.components.auth'));

  describe('Auth service', function(){
    it('is initialized correctly', inject(function(Auth) {
      expect(Auth).toBeDefined();
    }));
  });
});
