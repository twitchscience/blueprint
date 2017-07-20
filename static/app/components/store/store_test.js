describe('blueprint.components.store module', function() {
  beforeEach(module('blueprint.components.store'));

  describe('Store service', function(){
    it('is initialized correctly', inject(function(Store) {
      expect(Store).toBeDefined();
    }));
  });
});
