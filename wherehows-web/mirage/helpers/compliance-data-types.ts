import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

const getComplianceDataTypes = function(
  this: IFunctionRouteHandler,
  { complianceDataTypes }: { complianceDataTypes: any }
) {
  return {
    complianceDataTypes: this.serialize(complianceDataTypes.all()),
    status: ApiStatus.OK
  };
};

export { getComplianceDataTypes };
