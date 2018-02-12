import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

const getDatasetAccess = function(this: IFunctionRouteHandler, { accesses }: { accesses: any }) {
  return {
    access: this.serialize(accesses.first()),
    status: ApiStatus.OK
  };
};

export { getDatasetAccess };
