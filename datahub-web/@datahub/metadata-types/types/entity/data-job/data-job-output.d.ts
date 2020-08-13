/**
 * Outputs produced by the data job
 * @export
 * @interface IDataJobOutput
 * @link https://git.corp.linkedin.com:1367/a/plugins/gitiles/multiproducts/metadata-models/+/master/metadata-models/src/main/pegasus/com/linkedin/dataJob/DataJobOutput.pdsc
 */
export interface IDataJobOutput {
  // Output datasets produced by the data job during processing, each string is a dataset urn
  outputDatasets: Array<string>;
}
