angular.module('blueprint')
  .factory('Column', function() {
    // Design Note:
    // Hard-coding this should be good enough for now, but we could instead augment the Types
    // endpoint to obtain further meta-data, including if it's a mapping transformer type.
    var mappingTransformers = ['userIDWithMapping'];

    return {
      make: function() {
      return {
        InboundName: '',
        OutboundName: '',
        Transformer: 'varchar',
        size: 255,
        ColumnCreationOptions: '',
        mappingColumn: '', //For simplicity, we assume a single mapping column until we need more
        SupportingColumns: '',
        };
      },
      validate: function(column) {
        if (!column.InboundName || !column.OutboundName || !column.Transformer) {
          return false;
        } else if (column.Transformer == 'varchar' && !(column.size > 0 && column.size <= 65535)) {
          return false;
        }
        return true;
      },
      usingMappingTransformer: function(column) {
        return mappingTransformers.indexOf(column.Transformer) != -1;
      }
    }
  });
