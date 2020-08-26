import { IFunctionRouteHandler, IMirageRequest } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageDatasetCoreSchema } from '@datahub/datasets-core/types/vendor/mirage-for-datasets';
import { IDatasetComplianceInfo } from '@datahub/metadata-types/types/entity/dataset/compliance/info';

// TODO: [META-8403] Dataset compliance and compliance suggestions need to be expanded so that we can handle
// multiple compliance scenarios, but in this phase of development this should be sufficient to handle our
// single use case

/**
 * This handler is used by the mirage route config to handle the get request for dataset compliance
 */
export const getDatasetCompliance = function(
  this: IFunctionRouteHandler,
  schema: IMirageDatasetCoreSchema
): { complianceInfo: IDatasetComplianceInfo } {
  const complianceInfo: IDatasetComplianceInfo = {
    ...schema.db.datasetComplianceInfos[0],
    complianceEntities: schema.db.datasetComplianceAnnotationTags.where({ isSuggestion: false })
  };

  return { complianceInfo: this.serialize(complianceInfo) };
};

export const postDatasetCompliance = function(
  this: IFunctionRouteHandler,
  schema: IMirageDatasetCoreSchema,
  req: IMirageRequest
): void {
  const requestBody: IDatasetComplianceInfo = JSON.parse(req.requestBody);
  const annotations = requestBody.complianceEntities;

  schema.db.datasetComplianceAnnotationTags.remove({ isSuggestion: false });

  annotations.forEach(tag =>
    schema.db.datasetComplianceAnnotationTags.insert({
      ...tag,
      isSuggestion: false
    })
  );
};
