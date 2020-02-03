import { FabricType } from '@datahub/metadata-types/constants/common/fabric-type';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';

export interface IDatasetEntity {
  createdTime: number;
  decommissionTime: number | null;
  // Whether or not the dataset has been deprecated (slightly different from removed)
  deprecated: boolean | null;
  // Any note that was added when the dataset was deprecated
  deprecationNote: string | null;
  // Description of the dataset, if any
  description: string;
  // Fabric/environment the dataset exists in
  fabric: FabricType;
  // Timestamp for last modified time
  modifiedTime: number;
  // Human readable (ish) name for the dataset
  nativeName: string;
  nativeType: string;
  // Platform on which the dataset exists
  platform: DatasetPlatform;
  properties: string | null;
  // Whether the dataset has been removed
  removed: boolean;
  tags: Array<string>;
  // Equivalent to urn of a dataset
  uri: string;
}
