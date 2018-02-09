import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

const getDatasetSample = function(this: IFunctionRouteHandler, { samples }: { samples: any }) {
  return {
    sample: this.serialize(samples.first()),
    status: ApiStatus.OK
  };
};

export { getDatasetSample };
