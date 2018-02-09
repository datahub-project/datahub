import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

const getDatasetReferences = function(this: IFunctionRouteHandler, { references }: { references: any }) {
  return {
    references: this.serialize(references.all()),
    status: ApiStatus.OK
  };
};

export { getDatasetReferences };
