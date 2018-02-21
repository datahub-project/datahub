import { IUser } from 'wherehows-web/typings/api/authentication/user';
import { ApiStatus } from 'wherehows-web/utils/api';
import { DatasetPlatform } from 'wherehows-web/constants';

/**
 * Describes the properties of a Dataset object
 * @interface IDataset
 */
interface IDataset {
  created: number;
  formatedModified: string;
  hasSchemaHistory: boolean;
  hdfsBrowserLink: null | string;
  id: number;
  isFavorite: boolean;
  isOwned: boolean;
  isWatched: false;
  modified: number;
  name: string;
  owners: Array<IUser>;
  properties: null;
  schema: string; //JSON string
  source: string;
  urn: string;
  watchId: number;
}

/**
 * Describes the interface for a DatasetView. This represents a resource
 * derived from TMS (The Metadata Store)
 * @interface IDatasetView
 */
interface IDatasetView {
  platform: DatasetPlatform;
  nativeName: string;
  fabric: string; // get enum for fabric
  uri: string;
  description: string;
  nativeType: string;
  properties: string | null;
  tags: Array<string>;
  removed: boolean | null;
  deprecated: boolean | null;
  deprecationNote: string | null;
  createdTime: number;
  modifiedTime: number;
}

/**
 * Describes the response from the GET Dataset endpoint
 * @interface IDatasetGetResponse
 */
interface IDatasetGetResponse {
  status: ApiStatus;
  message?: string;
  dataset?: IDataset;
}

/**
 * Describes the interface of a response from the GET datasetView endpoint
 * @interface IDatasetViewGetResponse
 */
interface IDatasetViewGetResponse {
  status: ApiStatus;
  dataset?: IDatasetView;
}

/**
 * Describes the response from the GET /datasets api
 * @interface IDatasetsGetResponse
 */
interface IDatasetsGetResponse {
  total: number;
  start: number;
  count: number;
  elements: Array<IDatasetView>;
}

/**
 * Describes the options for the dataset
 * @interface IReadDatasetsOptionBag
 */
interface IReadDatasetsOptionBag {
  platform: DatasetPlatform | string;
  prefix: string;
}

export {
  IDatasetViewGetResponse,
  IDatasetView,
  IDatasetGetResponse,
  IDataset,
  IDatasetsGetResponse,
  IReadDatasetsOptionBag
};
