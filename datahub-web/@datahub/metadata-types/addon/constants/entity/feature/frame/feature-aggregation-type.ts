/**
 * Frame feature aggregation functions used for sliding window aggregation.
 * See https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Frame+User+Manual for more details
 * @export
 * @enum {string}
 */
export enum FeatureAggregationType {
  // Represents the SUM aggregation function to be applied on the data
  Sum = 'SUM',
  // Represents the COUNT aggregation function to be applied on the data
  Count = 'COUNT',
  // Represents the MAX aggregation function to be applied on the data
  Max = 'MAX',
  // Represents the TIMESINCE aggregation function to be applied on the data
  TimeSince = 'TIMESINCE',
  // Represents the AVG aggregation function to be applied on the data
  Avg = 'AVG',
  // Represents the LATEST aggregation function to be applied on the data
  Latest = 'LATEST'
}
