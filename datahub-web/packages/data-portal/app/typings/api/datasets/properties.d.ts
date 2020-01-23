import { ApiStatus } from '@datahub/utils/api/shared';

/**
 * Describes the interface a for dataset properties that are not sourced from pinot
 * @interface IDatasetProperties
 */
export interface IDatasetProperties {
  [prop: string]: any;
}

/**
 * Describes the interface for properties that are from sources other than pinot
 * @interface IDatasetPinotProperties
 */
export interface IDatasetPinotProperties {
  elements: Array<{
    columnNames: Array<string>;
    results: Array<string>;
  }>;
}

/**
 * Describes the interface for a response received from a GET request to the properties endpoint
 * when the source is NOT pinot
 * @interface IDatasetPropertiesGetResponse
 */
export interface IDatasetPropertiesGetResponse {
  status: ApiStatus;
  properties?: IDatasetProperties;
  message?: string;
}

/**
 * Describes the interface for a response received from a GET request to the properties endpoint
 * when the source IS pinot
 * @interface IDatasetPinotPropertiesGetResponse
 */
export interface IDatasetPinotPropertiesGetResponse {
  status: ApiStatus;
  properties?: IDatasetPinotProperties;
  message?: string;
}
