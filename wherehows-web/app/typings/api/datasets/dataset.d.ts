import { IUser } from 'wherehows-web/typings/api/authentication/user';
import { ApiStatus } from 'wherehows-web/utils/api';

/**
 * Describes the properties of a Dataset object
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
  schema: string;
  source: string;
  urn: string;
  watchId: number;
}

/**
 * Describes the response from the GET Dataset endpoint
 */
interface IDatasetGetResponse {
  status: ApiStatus;
  dataset?: IDataset;
}
