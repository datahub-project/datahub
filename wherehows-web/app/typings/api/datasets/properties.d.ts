import { ApiStatus } from 'wherehows-web/utils/api/shared';

/**
 * Describes the interface a for dataset properties that are not sourced from pinot
 * @interface IDatasetProperties
 */
interface IDatasetProperties {
  [prop: string]: any;
}

/**
 * Describes the interface for properties that are from sources other than pinot 
 * @interface IDatasetPinotProperties
 */
interface IDatasetPinotProperties {
  elements: Array<{
    columnNames: Array<string>;
    results: Array<string>;
  }>;
}

/**
 * Describes the interface for a response received from a GET request to the properties endpoint
 * when the source is NOT pinot
 * 
 * @interface IDatasetPropertiesGetResponse
 */
interface IDatasetPropertiesGetResponse {
  status: ApiStatus;
  properties?: IDatasetProperties;
  message?: string;
}

/**
 * Describes the interface for a response received from a GET request to the properties endpoint
 * when the source IS pinot
 * 
 * @interface IDatasetPinotPropertiesGetResponse
 */
interface IDatasetPinotPropertiesGetResponse {
  status: ApiStatus;
  properties?: IDatasetPinotProperties;
  message?: string;
}

export {
  IDatasetPropertiesGetResponse,
  IDatasetProperties,
  IDatasetPinotPropertiesGetResponse,
  IDatasetPinotProperties
};
