import { FabricType } from '@datahub/metadata-types/constants/common/fabric-type';

/**
 * Describes the interface for a DatasetOrigin mapping Fabric type values to internally descriptive
 * titles like HOLDEM, WAR
 * @export
 * @interface IDatasetOrigin
 */
export interface IDatasetOrigin {
  // Internal description for the various Fabrics
  displayTitle: string;
  // Maps the dataset URN fabric names
  origin: FabricType;
}

export type DatasetOrigins = Array<IDatasetOrigin>;

/**
 * Describes the api response shape when requesting dataset origins from the endpoint
 * @export
 * @interface DatasetOriginsResponse
 */
export interface IDatasetOriginsResponse {
  dataorigins: DatasetOrigins;
}
