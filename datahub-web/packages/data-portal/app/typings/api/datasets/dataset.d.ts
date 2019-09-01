import { IUser } from '@datahub/shared/api/user/authentication';
import { ApiStatus } from '@datahub/utils/api/shared';
import { ISharedOwner } from 'wherehows-web/typings/api/ownership/owner';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';

/**
 * Describes the properties of a Dataset object
 * @interface IDataset
 */
export interface IDataset {
  created: number | null;
  formatedModified: string | null;
  hasSchemaHistory: boolean;
  hdfsBrowserLink: null | string;
  id: number;
  isFavorite: boolean;
  isOwned: boolean;
  isWatched: boolean;
  modified: number | null;
  name: string;
  owners: Array<IUser> | null;
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
export interface IDatasetView {
  platform: DatasetPlatform;
  nativeName: string;
  fabric: string; // get enum for fabric
  uri: string;
  name: string;
  description: string;
  nativeType: string;
  properties: string | null;
  tags: Array<string>;
  removed: boolean | null;
  deprecated: boolean | null;
  deprecationNote: string;
  createdTime: number;
  modifiedTime: number;
  decommissionTime: number | null;
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
export interface IDatasetViewGetResponse {
  status: ApiStatus;
  dataset?: IDatasetView;
}

/**
 * Describes the response from the GET /datasets api
 * @interface IDatasetsGetResponse
 */
export interface IDatasetsGetResponse {
  total: number;
  start: number;
  count: number;
  elements: Array<IDatasetView>;
}

/**
 * Dataset snapshot will contain all subtypes of dataset
 * like compliance, ump.
 *
 * For now only contains UMP
 */
export interface IDatasetSnapshot {
  Ownership: Array<ISharedOwner>;
}

export interface IDatasetMetricSnapshot {
  Ownership: Array<ISharedOwner>;
}
