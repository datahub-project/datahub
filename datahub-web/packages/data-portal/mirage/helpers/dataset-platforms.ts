import { IFunctionRouteHandler } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageWherehowsDBs } from 'wherehows-web/typings/ember-cli-mirage';
import { IDataPlatform } from '@datahub/metadata-types/types/entity/dataset/platform';

interface IReturnType {
  platforms: Array<IDataPlatform>;
}
export const getDatasetPlatforms = function(
  this: IFunctionRouteHandler,
  { platforms }: IMirageWherehowsDBs
): IReturnType {
  return {
    platforms: this.serialize(platforms.all()) as Array<IDataPlatform>
  };
};
