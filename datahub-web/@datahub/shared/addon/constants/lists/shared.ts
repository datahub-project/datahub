import { FeatureEntity } from '@datahub/data-models/entity/feature/feature-entity';
import { DataModelName } from '@datahub/data-models/constants/entity/index';
import { MockEntity } from '@datahub/data-models/entity/mock/mock-entity';

/**
 * Lists entities that have Entity List support
 * note: DatasetEntity is excluded from type pending mid-tier support for urn attribute, support for uri would be throw away
 */
export const supportedListEntities: Array<DataModelName | typeof MockEntity.displayName> = [FeatureEntity.displayName];

/**
 * Enumerates the cta text for toggling an Entity off or onto a list for action triggers where List toggle actions are called
 */
export enum ListToggleCta {
  remove = 'Remove from list',
  add = 'Add to list'
}
