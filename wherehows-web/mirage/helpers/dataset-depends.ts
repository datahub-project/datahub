import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

const getDatasetDepends = function(this: IFunctionRouteHandler, { depends }: { depends: any }) {
  return {
    depends: this.serialize(depends.all()),
    status: ApiStatus.OK
  };
};

export { getDatasetDepends };
