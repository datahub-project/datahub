import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

const getDatasetComplianceSuggestion = function(
  this: IFunctionRouteHandler,
  { complianceSuggestions }: { complianceSuggestions: any }
) {
  return {
    complianceSuggestion: this.serialize(complianceSuggestions.first()),
    status: ApiStatus.OK
  };
};

export { getDatasetComplianceSuggestion };
