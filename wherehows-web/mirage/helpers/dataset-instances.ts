import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

const getDatasetInstances = function(this: IFunctionRouteHandler, { instances }: { instances: any }) {
  return {
    instances: this.serialize(instances.all()),
    status: ApiStatus.OK
  };
};

export { getDatasetInstances };
