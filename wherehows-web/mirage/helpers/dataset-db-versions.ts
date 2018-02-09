import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

const getDatasetDbVersions = function(this: IFunctionRouteHandler, { versions }: { versions: any }) {
  return {
    versions: this.serialize(versions.all()),
    status: ApiStatus.OK
  };
};

export { getDatasetDbVersions };
