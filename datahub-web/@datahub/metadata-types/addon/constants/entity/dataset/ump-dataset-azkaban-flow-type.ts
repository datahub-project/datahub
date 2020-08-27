/**
 * Azkaban flow type
 * @export
 * @namespace Dataset
 * @enum {string}
 */
export enum UMPDatasetAzkabanFlowType {
  // Choose this option if Azkaban project is in production
  Production = 'PRODUCTION',
  // Choose this option if Azkaban project is in backfill
  BackFill = 'BACKFILL'
}
