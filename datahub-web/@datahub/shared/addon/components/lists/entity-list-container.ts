import WithEntityLists from '@datahub/shared/components/lists/with-entity-lists';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/lists/entity-list-container';
import { layout, tagName } from '@ember-decorators/component';
import { DataModelEntity, DataModelName, DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { computed, action } from '@ember/object';
import { findEntityInList } from '@datahub/shared/utils/lists';
import { alias, map, gt } from '@ember/object/computed';
import { capitalize } from 'lodash';
import { IEntityLinkAttrs } from '@datahub/data-models/types/entity/shared';
import { singularize } from 'ember-inflector';
import { Snapshot } from '@datahub/metadata-types/types/metadata/snapshot';
import { IStoredEntityAttrs } from '@datahub/shared/types/lists/list';
import { set } from '@ember/object';
import { setProperties } from '@ember/object';
import Notifications from '@datahub/utils/services/notifications';
import { inject as service } from '@ember/service';
import { NotificationEvent } from '@datahub/utils/constants/notifications';
import { task } from 'ember-concurrency';
import { ETask } from '@datahub/utils/types/concurrency';

/**
 * Defines the interface for the output of the EntityList mapping predicate function
 * Outputs an entity with its generated link attributes
 */
interface IEntityWithLink {
  // References the entity for which the link attributes should be generated
  entity: DataModelEntityInstance;

  // Generated link attributes for the aforementioned entity
  linkAttr?: IEntityLinkAttrs;
}

/**
 * Container component for a list of Entities
 * Provides actions for the entire list and data transformation
 * @export
 * @class EntityListContainer
 * @extends {WithEntityLists}
 */
@layout(template)
@tagName('')
export default class EntityListContainer extends WithEntityLists {
  /**
   * DataHub host application notifications service
   */
  @service('notifications')
  notificationsService?: Notifications;

  /**
   * The type of DataModel entity being rendered
   */
  entityType?: DataModelName;

  /**
   * Retains a subset of the currently selected DataModelEntity types from the items in the EntityList
   * This list is a non durable list because operations rely on object references for referential integrity
   * If persistence is required for this list, then referencing items between other lists will requires
   * more durable reference, for example using entity urns
   */
  selectedEntities: Array<DataModelEntityInstance> = [];

  /**
   * A reference to the Entity class
   * Required for the computed property dependent computed property entityListTrampoline
   * @readonly
   */
  @computed('entityType')
  get entity(): DataModelEntity | null {
    const { entityType } = this;
    return entityType ? DataModelEntity[entityType] : null;
  }

  /**
   * Singularized entity name
   * @readonly
   */
  @computed('entity')
  get name(): string {
    const { entity } = this;
    return entity ? singularize(entity.displayName) : '';
  }

  /**
   * Name of the list header
   * @readonly
   */
  @computed('name')
  get listName(): string {
    const { name } = this;
    return name ? `${capitalize(name)} list` : '';
  }

  /**
   * Number of items mapped from the source list
   */
  @alias('entityListWithLinkAttrs.length')
  listCount?: number;

  /**
   * Flag indicating that more than one entity from the list is currently selected
   */
  @gt('selectedEntities.length', 1)
  hasMultipleSelected!: boolean;

  /**
   * Flag indicating that all the items in the list have been selected
   * Weak comparison on list length equality, suffices for the moment and
   * cheaper than a full value equality or deep equality check which may come at cost for longer lists
   * @readonly
   */
  @computed('listCount', 'selectedEntities.length')
  get selectedAll(): boolean {
    const {
      selectedEntities: { length },
      listCount
    } = this;
    // Exclude zero length list
    return Boolean(length) && listCount === length;
  }

  /**
   * References the associated entity list
   * @readonly
   */
  @alias('entityListTrampoline.list')
  list!: ReadonlyArray<IStoredEntityAttrs>;

  /**
   * Hydrated list of entities populated on initialization from the urns serialized in persistent storage
   */
  instances: Array<DataModelEntityInstance> = [];

  /**
   * For each entity in the entity list, outputs a decoration with IEntityLinkAttrs instance
   * This allows linking the list entity to the entity page
   */
  @map('instances', function(this: EntityListContainer, instance: DataModelEntityInstance): IEntityWithLink {
    // The entity class (statics) associated with this container
    const { entity: entityType } = this;
    const entityWithLink: IEntityWithLink = { entity: instance };

    if (entityType && instance) {
      const linkAttr = instance.entityLink;

      return linkAttr ? { ...entityWithLink, linkAttr } : entityWithLink;
    }

    return entityWithLink;
  })
  entityListWithLinkAttrs!: Array<IEntityWithLink>;

  /**
   * Toggles a selected state when an entity in the list is selected or deselected in the ui
   * Toggle action is determined based on presence of entity in selection subset: selectedEntities
   * i.e. if absent added, otherwise removed
   */
  @action
  onSelectEntity(selectedEntity: DataModelEntityInstance): void {
    const { selectedEntities } = this;
    const isInSelection = findEntityInList(selectedEntities)(selectedEntity) as DataModelEntityInstance;

    isInSelection ? selectedEntities.removeObject(isInSelection) : selectedEntities.addObject(selectedEntity);
  }

  /**
   * Handles the change event for the list (group) checkbox element
   * Toggling to checked will add all items in the list, otherwise clear the list
   */
  @action
  onSelectEntityList(): void {
    const { selectedEntities, instances } = this;
    this.selectedAll
      ? // Remove all items from selected entities list
        selectedEntities.setObjects([])
      : // Add all items to list
        // If there are items in the list , then replace the selected entities list with all items
        instances.length && selectedEntities.setObjects([...instances]);
  }

  /**
   * Handler removes all the selected items from the entity list and the selected entities list
   */
  @action
  removeAllSelected(): void {
    const { selectedEntities, listsManager, notificationsService, entity } = this;
    const selectedCount = selectedEntities.length;
    const notificationMessage = entity ? `${selectedCount} ${entity.displayName} removed from list successfully` : '';

    // Remove entities from lists manager service
    listsManager && listsManager.removeFromList(selectedEntities);
    // Also remove from component selected list
    selectedEntities.setObjects([]);

    // On removal, refresh instances

    this.hydrateEntitiesTask.perform();

    notificationsService &&
      notificationsService.notify({
        type: NotificationEvent.success,
        content: notificationMessage
      });
  }

  /**
   * Reads the list of urn stored in persistent storage and then batch queries the remote for snapshots and entities for hydration
   * then populates a local attribute with the received list of entities
   */
  @(task(function*(
    this: EntityListContainer
  ): IterableIterator<Promise<Array<Snapshot>> | Promise<Array<DataModelEntityInstance>>> {
    const { entity } = this;
    // Extract urns from the serialization list
    const urns = (this.list || []).map(({ urn }): string => urn);

    if (entity && urns.length) {
      // Hydrate entity instances with Snapshot and IBaseEntity attributes
      const snapshots = ((yield entity.readSnapshots(urns)) as unknown) as Array<Snapshot>;
      // IBaseEntity property (entity) hydration happens in an async iteration because attributes batch GET endpoint for entities is N/A currently
      const instances = ((yield Promise.all(
        snapshots.map(
          async (snapshot): Promise<DataModelEntityInstance> => {
            const listEntity = new entity(snapshot.urn);
            setProperties(listEntity, { snapshot, entity: await listEntity.readEntity });

            return listEntity;
          }
        )
      )) as unknown) as Array<DataModelEntityInstance>;

      return set(this, 'instances', instances);
    }

    return set(this, 'instances', []);
  }).restartable())
  hydrateEntitiesTask!: ETask<void>;

  /**
   * On initialization, hydrate the entities list with data serialized in persistent storage
   */
  init(): void {
    super.init();

    this.hydrateEntitiesTask.perform();
  }
}
