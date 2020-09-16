// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/entity-page/entity-page-content/nacho-table';
import { layout, tagName } from '@ember-decorators/component';
import { INachoTableComponent } from '@datahub/data-models/types/entity/rendering/page-components';
import { assertComponentPropertyNotUndefined } from '@datahub/utils/decorators/assert';
import BasePageComponent from '@datahub/shared/components/entity-page/base-page-component';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity/index';
import { KeyNamesWithValueType } from '@datahub/utils/types/base';

/**
 * Nacho table wrapper for generic entity page
 *
 * @export
 * @class EntityPageContentNachoTable
 * @template E entity to render
 */

@tagName('')
@layout(template)
export default class EntityPageContentNachoTable<E> extends BasePageComponent {
  /**
   * See interface INachoTableComponent
   */
  @assertComponentPropertyNotUndefined
  options!: INachoTableComponent<E>['options'];

  /**
   * Will get the data to render within the table.
   * If no propertyName specified, then entity will be used
   *
   * @readonly
   */
  get data(): Array<unknown> | undefined {
    const { entity, options } = this;
    const { propertyName } = options;
    const propertyNameWithType = propertyName as KeyNamesWithValueType<DataModelEntityInstance, Array<unknown>>;
    if (propertyNameWithType) {
      return entity[propertyNameWithType];
    }
    return [entity];
  }
}
