import { IFunctionRouteHandler } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageDatasetCoreSchema } from '@datahub/datasets-core/types/vendor/mirage-for-datasets';
import { IDatasetComplianceSuggestionInfo } from '@datahub/data-models/types/entity/dataset';

// TODO: [META-8403] Dataset compliance and compliance suggestions need to be expanded so that we can handle
// multiple compliance scenarios, but in this phase of development this should be sufficient to handle our
// single use case

/**
 * This handler is used by the mirage route config to handle the get request for dataset compliance suggestions
 */
export const getDatasetComplianceSuggestions = function(
  this: IFunctionRouteHandler,
  schema: IMirageDatasetCoreSchema
): { complianceSuggestion: IDatasetComplianceSuggestionInfo } {
  const suggestionInfo: IDatasetComplianceSuggestionInfo = {
    lastModified: 1542929020775,
    suggestedDatasetClassification: {},
    urn: '',
    suggestedFieldClassification: schema.db.datasetComplianceAnnotationTags.slice(10, 19).map(annotation => ({
      uid: 'fake-uid',
      confidenceLevel: 0.9,
      suggestion: annotation
    }))
  };

  return { complianceSuggestion: suggestionInfo };
};
