/**
 * Inputs consumed by the data job
 * @export
 * @interface IDataJobInput
 * @link https://git.corp.linkedin.com:1367/a/plugins/gitiles/multiproducts/metadata-models/+/master/metadata-models/src/main/pegasus/com/linkedin/dataJob/DataJobInput.pdsc
 */
export interface IDataJobInput {
  // Input datasets consumed by the data job during processing, each string is a dataset urn
  inputDatasets: Array<string>;
}
