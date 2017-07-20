describe('blueprint.components.rest module', function() {
  beforeEach(module('blueprint.components.rest'));

  describe('Event factory', function(){
    it('is initialized correctly', inject(function(Event) {
      expect(Event).toBeDefined();
    }));
  });

  describe('Schema factory', function(){
    it('is initialized correctly', inject(function(Schema) {
      expect(Schema).toBeDefined();
    }));
  });

  describe('KinesisConfig factory', function(){
    it('is initialized correctly', inject(function(KinesisConfig) {
      expect(KinesisConfig).toBeDefined();
    }));
  });

  describe('Types factory', function(){
    it('is initialized correctly', inject(function(Types) {
      expect(Types).toBeDefined();
    }));
  });

  describe('Droppable factory', function(){
    it('is initialized correctly', inject(function(Droppable) {
      expect(Droppable).toBeDefined();
    }));
  });

  describe('EventMetadata factory', function(){
    it('is initialized correctly', inject(function(EventMetadata) {
      expect(EventMetadata).toBeDefined();
    }));
  });

  describe('Suggestions factory', function(){
    it('is initialized correctly', inject(function(Suggestions) {
      expect(Suggestions).toBeDefined();
    }));
  });

  describe('Maintenance factory', function(){
    it('is initialized correctly', inject(function(Maintenance) {
      expect(Maintenance).toBeDefined();
    }));
  });

  describe('SchemaMaintenance factory', function(){
    it('is initialized correctly', inject(function(SchemaMaintenance) {
      expect(SchemaMaintenance).toBeDefined();
    }));
  });

  describe('Stats factory', function(){
    it('is initialized correctly', inject(function(Stats) {
      expect(Stats).toBeDefined();
    }));
  });
});
