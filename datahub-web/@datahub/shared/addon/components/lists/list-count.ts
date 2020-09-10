import WithEntityLists from '@datahub/shared/components/lists/with-entity-lists';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/lists/list-count';
import { layout, classNames, className } from '@ember-decorators/component';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { computed } from '@ember/object';
import { singularize } from 'ember-inflector';
import { alias } from '@ember/object/computed';
import { capitalize } from '@ember/string';

export const baseComponentClass = `entity-list-count`;

/**
 * ListCount is a bug for displaying the number of items in a list
 * The bug should be displayed when there are items in the list and hidden or dismissed otherwise
 * @export
 * @class ListCount
 * @extends {WithEntityLists}
 */
@layout(template)
@classNames(baseComponentClass)
export default class ListCount extends WithEntityLists {
  baseComponentClass = baseComponentClass;

  /**
   * The type of DataModel entity being rendered
   */
  entityType?: DataModelEntity;

  /**
   * Reference to the entity class
   * Required for entity list trampoline
   */
  @alias('entityType')
  entity?: DataModelEntity;

  /**
   * Display value for the current list item
   * @readonly
   */
  @computed('entityType')
  get displayName(): string {
    const { entityType } = this;
    return entityType ? `${capitalize(singularize(entityType.displayName))} List` : '';
  }

  /**
   * The number of items in the list
   */
  @className(`${baseComponentClass}--display`, `${baseComponentClass}--dismiss`)
  @alias('entityListTrampoline.list.length')
  count?: number;
}
