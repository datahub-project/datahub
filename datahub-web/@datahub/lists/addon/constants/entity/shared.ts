import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';

// Alias for a DataModelEntity type in the list of supportedListEntities
export type SupportedListEntity = Exclude<DataModelEntity, typeof DatasetEntity>;

/**
 * Lists entities that have Entity List support
 * note: DatasetEntity is excluded from type pending mid-tier support for urn attribute, support for uri would be throw away
 */
export const supportedListEntities: Array<SupportedListEntity> = [PersonEntity];

/**
 * Enumerates the cta text for toggling an Entity off or onto a list for action triggers where List toggle actions are called
 */
export enum ListToggleCta {
  remove = 'Remove from list',
  add = 'Add to list'
}
