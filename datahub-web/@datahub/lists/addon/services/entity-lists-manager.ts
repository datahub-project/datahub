import Service from '@ember/service';
import StorageArray from 'ember-local-storage/local/array';
import { findEntityInList, serializeForStorage } from '@datahub/lists/utils';
import { storageFor } from 'ember-local-storage';
import { computed } from '@ember/object';
import { supportedListEntities, SupportedListEntity } from '@datahub/lists/constants/entity/shared';
import { noop } from '@datahub/utils/function/noop';
import { IStoredEntityAttrs } from '@datahub/lists/types/list';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { DataModelEntityInstance } from '@datahub/data-models/addon/constants/entity';

// Map of List Entity displayName to list of instances
type ManagedListEntities = Record<SupportedListEntity['displayName'], ReadonlyArray<IStoredEntityAttrs>>;

/**
 * Entity Lists Management service, handles operations, state management and shares state of Entity lists
 * Interfaces with the storage layer to persist list state and serialization from storage
 * @export
 * @class EntityListsManager
 * @extends {Service}
 */
export default class EntityListsManager extends Service {
  /**
   * Creates a computed array proxy of DataModel entity attributes (IStoredEntityAttrs), the proxy is persisted to localStorage
   * localStorage objects are serialized as JSON, therefore, the de-serialization step will
   * be an instantiation of the related type and hydration from a source of truth endpoint
   * @private
   */
  @storageFor('entity-list')
  private readonly entityStorageProxy!: StorageArray<IStoredEntityAttrs>;

  /**
   * Lists the entities that are supported for the application lists feature
   */
  supportedListEntities = supportedListEntities;

  /**
   * Mapped Entity Lists for supported DataModelEntity types computed from persisted data in local storage
   * @memberof EntityListsManager
   */
  @computed('entityStorageProxy.[]')
  get entities(): ManagedListEntities {
    // Initialize with empty lists, these will be overridden in the reduction over supportedListEntities
    const entityMap = {
      [PersonEntity.displayName]: []
    };
    const entityList = this.entityStorageProxy;

    return this.supportedListEntities.reduce((entityMap, EntityType: SupportedListEntity): ManagedListEntities => {
      // entityList is a single list of all supported entity instances
      // Filter out entities that match the EntityType
      // Create a new instance to hydrate with the saved snapshot and baseEntity
      const storedEntities = entityList.filter((storedEntity): boolean => storedEntity.type === EntityType.displayName);

      return { ...entityMap, [EntityType.displayName]: Object.freeze(storedEntities) };
    }, entityMap);
  }

  /**
   * Private utility function to toggle between adding or removing entities from the storageProxy
   * @private
   * @memberof EntityListsManager
   */
  private updateList(
    updateType: 'add' | 'remove'
  ): (entities: DataModelEntityInstance | Array<DataModelEntityInstance>) => this {
    return (entities: DataModelEntityInstance | Array<DataModelEntityInstance>): this => {
      const entitiesToUpdate: Array<DataModelEntityInstance> = [].concat.apply(entities);
      const storageProxy = this.entityStorageProxy;

      if (storageProxy) {
        let updateStrategy: {
          filterMap: (arg: Array<DataModelEntityInstance>) => Array<IStoredEntityAttrs>;
          updater: (objects: Array<IStoredEntityAttrs>) => StorageArray<IStoredEntityAttrs>;
        } = { filterMap: noop, updater: noop };

        if (updateType === 'add') {
          updateStrategy = {
            updater: storageProxy.addObjects,
            filterMap: (entities): Array<IStoredEntityAttrs> =>
              entities
                .filter((entity: DataModelEntityInstance): boolean => !findEntityInList(storageProxy)(entity))
                .map(serializeForStorage)
          };
        }

        if (updateType === 'remove') {
          updateStrategy = {
            updater: storageProxy.removeObjects,
            filterMap: (entities): Array<IStoredEntityAttrs> => storageProxy.filter(findEntityInList(entities))
          };
        }

        const resolvedEntities = updateStrategy.filterMap(entitiesToUpdate);
        resolvedEntities.length && updateStrategy.updater.call(storageProxy, resolvedEntities);
      }

      return this;
    };
  }

  /**
   * Filters an entity or a list of entities in entity list.
   * A list of contained / found entities is returned, if match(es) is/are found, if the list is empty then
   * no match or matches exist or a storage list does not exist
   */
  findInList(entities: DataModelEntityInstance | Array<DataModelEntityInstance>): Array<DataModelEntityInstance> {
    const entitiesToFind: Array<DataModelEntityInstance> = [].concat.apply(entities);
    const storageProxy = this.entityStorageProxy;

    return storageProxy
      ? entitiesToFind.filter((entity: DataModelEntityInstance):
          | IStoredEntityAttrs
          | DataModelEntityInstance
          | undefined => findEntityInList(storageProxy)(entity))
      : [];
  }

  /**
   * Adds one or more DataModelEntityInstances to the DataModel list with a matching type
   * @memberof EntityListsManager
   */
  addToList: (entities: DataModelEntityInstance | Array<DataModelEntityInstance>) => this = this.updateList('add');

  /**
   * Removes one or more DataModelEntityInstances from the related DataModel list
   * @memberof EntityListsManager
   */
  removeFromList: (entities: DataModelEntityInstance | Array<DataModelEntityInstance>) => this = this.updateList(
    'remove'
  );
}

declare module '@ember/service' {
  //eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    'entity-lists-manager': EntityListsManager;
  }
}
