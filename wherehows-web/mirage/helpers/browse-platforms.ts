import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import browsePlatforms from 'wherehows-web/mirage/fixtures/browse-platforms';

export const getBrowsePlatforms = function(this: IFunctionRouteHandler) {
  return this.serialize(browsePlatforms);
};
