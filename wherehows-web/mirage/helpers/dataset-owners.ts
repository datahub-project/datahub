import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

const getDatasetOwners = function(this: IFunctionRouteHandler, { owners }: { owners: any }) {
  return {
    owners: this.serialize(owners.all()),
    status: ApiStatus.OK
  };
};

export { getDatasetOwners };
