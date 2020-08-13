import { IAzkabanJobAspect } from '@datahub/metadata-types/types/entity/data-job/azkaban/job/aspect';

/**
 * A metadata snapshot for a specific azkaban job entity
 * @export
 * @interface IAzkabanJobSnapshot
 * @link https://git.corp.linkedin.com:1367/a/plugins/gitiles/multiproducts/metadata-models/+/master/metadata-models/src/main/pegasus/com/linkedin/metadata/snapshot/AzkabanJobSnapshot.pdsc
 */
export interface IAzkabanJobSnapshot {
  // Azkaban job urn identifying a Azkaban declared job
  urn: string;
  // The list of metadata aspects associated with a Azkaban job. Depending on the use case, this can either be all, or a selection, of supported aspects.
  aspects: Array<IAzkabanJobAspect>;
}
