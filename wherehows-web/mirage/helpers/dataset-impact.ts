import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

const getDatasetImpact = function(this: IFunctionRouteHandler, { impacts }: { impacts: any }) {
  return {
    impact: this.serialize(impacts.first()),
    status: ApiStatus.OK
  };
};

export { getDatasetImpact };
