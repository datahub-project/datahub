import { IFunctionRouteHandler } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';

export const getBrowsePlatforms = function(this: IFunctionRouteHandler): Array<string> {
  return this.serialize(
    Object.values(DatasetPlatform)
      .sort()
      .map(platform => `[platform=${platform}]`)
  );
};
