import Component from '@ember/component';
import EntityListsManager from '@datahub/shared/services/entity-lists-manager';
import ComputedProperty from '@ember/object/computed';
import EmberObject, { computed } from '@ember/object';
import { alias } from '@ember/object/computed';
import { inject as service } from '@ember/service';
import { IEntityListTrampoline, IStoredEntityAttrs } from '@datahub/shared/types/lists/list';
import { DataModelEntity } from '@datahub/data-models/constants/entity';

/**
 * Returns a Computed Property that retrieves the displayName of an entity and uses it to construct
 * the dependent key value on an EmberObject this allows the same component property to support
 * different DataModelEntity types dynamically on the same key
 * @template T the component on which the trampoline is used as a computed property
 * @returns {ComputedProperty<IEntityListTrampoline>}
 */
const getTrampoline = function<
  T extends { entity?: { displayName: DataModelEntity['displayName'] }; listsManager: EntityListsManager }
>(): ComputedProperty<IEntityListTrampoline> {
  return computed('entity', 'listsManager', {
    /**
     * Creates a trampoline (proxy) with a computed property based on the value of the entity display name
     */
    get(this: T): IEntityListTrampoline {
      const { entity, listsManager } = this;

      if (entity && listsManager) {
        const { displayName } = entity;

        class Trampoline extends EmberObject {
          @alias(`listsManager.entities.${displayName}.[]`)
          list!: ReadonlyArray<IStoredEntityAttrs>;
        }

        return Trampoline.create({ listsManager });
      }

      throw new Error('Missing Entity / listsManager Service reference');
    }
  });
};

/**
 * Shared attributes for components that interact with the lists manager service and entity list trampoline computed property
 * @export
 * @class WithEntityLists
 * @extends {Component}
 */
export default class WithEntityLists extends Component {
  /**
   * References the EntityListsManager service
   */
  @service('entity-lists-manager')
  listsManager?: EntityListsManager;

  /**
   * Computed 'jump' to the specific Entity list on the listManager related to the entityType for this container
   * Based on the entityType, give a computed property that depends on the entity types display name
   */
  @getTrampoline()
  entityListTrampoline!: IEntityListTrampoline;
}
