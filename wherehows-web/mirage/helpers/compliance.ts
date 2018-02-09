import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

const getDatasetCompliance = function(this: IFunctionRouteHandler, { compliances }: { compliances: any }) {
  return {
    complianceInfo: this.serialize(compliances.first()),
    status: ApiStatus.OK
  };
};

export { getDatasetCompliance };
