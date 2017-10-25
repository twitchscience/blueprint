angular.module('blueprint', [
  'ngRoute',
  'blueprint.components.auth',
  'blueprint.components.store',
  'blueprint.kinesisconfig.create',
  'blueprint.kinesisconfig.list',
  'blueprint.kinesisconfig.show',
  'blueprint.schema.create',
  'blueprint.schema.list',
  'blueprint.schema.show',
  'blueprint.stats',
  'tandibar/ng-rollbar'
]);
