/**
 * Represents a Frame HDFS source and related properties
 * @export
 * @interface IHDFSSourceProperties
 */
export interface IHDFSSourceProperties {
  // "File URI (HDFS path) of the dataset, or Dali URI (dalids:///)
  path: string;
  // Extra parameters for Frame's generic data manipulation
  extraParameters?: Record<string, string | Array<string>>;
  // Set as true to identify a fact dataset for aggregation
  // default false
  timeseries: boolean;
  // Field name of timestamp column in fact data
  timestamp?: string;
  // Format of the timestamp value, in java.time.DateTimeFormatter format
  timestampFormat?: string;
  // True or false to specify if the source is pointing to a time-sensitive path
  hasTimeSnapshot: boolean;
}
