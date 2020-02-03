import { IDatasetSchemaColumn } from '@datahub/metadata-types/types/entity/dataset/scehma';
import { IDatasetEntity } from '@datahub/metadata-types/types/entity/dataset/dataset-entity';
import { IDataPlatform } from '@datahub/metadata-types/types/entity/dataset/platform';

type SchemaDb<T> = Array<T> & {
  where: (query: Partial<T>) => Array<T>;
  update: (query: Partial<T>) => void;
  remove: (query: Partial<T>) => void;
  insert: (item: T) => void;
};

/**
 * Used a backwards approach to develop this interface, which represents the data available in the route
 * handler function for our mirage configurations. Examined the object and saw that this was the format of
 * the available interfaces, and mapping them here specifically for this addon.
 */
export interface IMirageDatasetCoreSchema {
  db: {
    datasetSchemaColumns: SchemaDb<IDatasetSchemaColumn>;
    datasetViews: SchemaDb<IDatasetEntity>;
    platforms: SchemaDb<IDataPlatform>;
  };
}
