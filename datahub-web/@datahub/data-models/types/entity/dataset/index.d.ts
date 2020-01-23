import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
/**
 * Describes the options for the dataset
 * @interface IReadDatasetsOptionBag
 */
export interface IReadDatasetsOptionBag {
  platform: DatasetPlatform | string;
  prefix: string;
  start?: number;
}
