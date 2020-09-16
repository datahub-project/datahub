import { IAzkabanFlowAspect } from '@datahub/metadata-types/types/entity/data-job/azkaban/flow/aspect';

/**
 * A metadata snapshot for a specific azkaban flow entity
 * @export
 * @interface IAzkabanFlowSnapshot
 * @link https://git.corp.linkedin.com:1367/a/plugins/gitiles/multiproducts/metadata-models/+/master/metadata-models/src/main/pegasus/com/linkedin/metadata/snapshot/AzkabanFlowSnapshot.pdsc
 */
export interface IAzkabanFlowSnapshot {
  // Azkaban flow urn identifying a Azkaban declared flow
  urn: string;
  // The list of metadata aspects associated with a Azkaban flow. Depending on the use case, this can either be all, or a selection, of supported aspects.
  aspects: Array<IAzkabanFlowAspect>;
}
