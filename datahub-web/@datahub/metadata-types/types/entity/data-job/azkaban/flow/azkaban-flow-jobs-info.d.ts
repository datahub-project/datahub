/**
 * Azkaban jobs associated with a Azkaban flow
 * @export
 * @interface IAzkabanFlowJobsInfo
 * @link https://git.corp.linkedin.com:1367/a/plugins/gitiles/multiproducts/metadata-models/+/master/metadata-models/src/main/pegasus/com/linkedin/dataJob/azkaban/AzkabanFlowJobsInfo.pdsc
 */
export interface IAzkabanFlowJobsInfo {
  // List of Azkaban jobs that are part of this flow
  jobs: Array<string>;
}
