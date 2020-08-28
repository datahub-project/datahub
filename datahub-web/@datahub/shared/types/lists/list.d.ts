import EntityListsManager from '@datahub/shared/services/entity-lists-manager';
import EmberObject from '@ember/object';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';

/**
 * Return type for the Computed Property EntityListTrampoline
 */
export interface IEntityListTrampoline extends EmberObject {
  // The list of IStoredEntityAttrs to monitor
  list: ReadonlyArray<IStoredEntityAttrs>;
  // A reference to the listsManager service to interact with list states
  listsManager?: EntityListsManager;
}

/**
 * Describes the interface for the attributes that are stored in persistent storage representing a DataModelEntityInstance
 * @export
 */
export interface IStoredEntityAttrs {
  // DataModelEntityInstance urn representing a reference to the data model instance that's stored
  urn: DataModelEntityInstance['urn'];
  // A reference to the type of the DataModelEntityInstance. Useful for rehydrating an instance when reading from the store
  type: DataModelEntityInstance['displayName'];
}
