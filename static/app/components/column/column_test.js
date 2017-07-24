describe('blueprint.components.column module', function() {
  beforeEach(module('blueprint.components.column'));

  describe('Column service', function(){
    it('is initialized correctly', inject(function(Column) {
      expect(Column).toBeDefined();
    }));
  });
});
