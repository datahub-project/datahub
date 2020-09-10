import WithEntityLists from '@datahub/shared/components/lists/with-entity-lists';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/lists/toggle-on-list';
import { layout, tagName, classNames, className } from '@ember-decorators/component';
import { computed } from '@ember/object';
import { findEntityInList } from '@datahub/shared/utils/lists';
import { ListToggleCta } from '@datahub/shared/constants/lists/shared';
import { noop } from 'lodash';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';

export const baseComponentClass = 'entity-list-toggle';
const buttonClass = 'nacho-button nacho-button--secondary nacho-button--small';

/**
 * ui component to add or remove an entity from its related list, also indicates if it's
 * associated entity is in that entity list
 * @export
 * @class ToggleOnList
 * @extends {WithEntityLists}
 */
@layout(template)
@classNames(baseComponentClass, buttonClass)
@tagName('button')
export default class ToggleOnList extends WithEntityLists {
  /**
   * An instance of the data model entity to add or remove from the list
   */
  entity?: DataModelEntityInstance;

  /**
   * External action to notify external entity of DataModelInstance toggled on or off
   */
  didToggleEntity: (arg?: DataModelEntityInstance) => void = noop;

  /**
   * Computed flag indicating the associate entity exists in the related entity list
   * @readonly
   * @type {boolean}
   * @memberof ToggleOnList
   */
  @className(`${baseComponentClass}--remove`, `${baseComponentClass}--add`)
  @computed('entityListTrampoline.list')
  get entityExistsInList(): boolean {
    const {
      entityListTrampoline: { list },
      entity
    } = this;
    return entity && list ? Boolean(findEntityInList([...list])(entity)) : false;
  }

  /**
   * The call to action for the button toggle
   * @readonly
   */
  @computed('entityExistsInList')
  get cta(): ListToggleCta {
    return this.entityExistsInList ? ListToggleCta.remove : ListToggleCta.add;
  }

  /**
   * Handles the click event by toggling the associated entity onto or off the related entity list
   * Toggle is dependent on presence or absence from the entity list
   */
  click(): void {
    const { listsManager, entity, entityExistsInList } = this;
    if (listsManager && entity) {
      entityExistsInList ? listsManager.removeFromList(entity) : listsManager.addToList(entity);
      this.didToggleEntity();
    }
  }
}
