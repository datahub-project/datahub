import Component from '@ember/component';
import { tagName } from '@ember-decorators/component';
import { computed, get } from '@ember/object';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { ISearchEntityRenderProps } from '@datahub/data-models/types/search/search-entity-render-prop';
import { SupportedValue } from '@datahub/utils/components/properties/value-renderer';

/**
 * Represent a search field value. It can render a list of items,
 * a single item.
 *
 * Also, by using SearchFieldValue, it supports to pass a custom component to use
 */
@tagName('')
export default class SearchSearchField extends Component {
  /**
   * Metadata related to the field to render
   */
  field: ISearchEntityRenderProps;

  /**
   * The entity to read the field from
   */
  entity: DataModelEntity;

  /**
   * Extracted value from entity
   */
  @computed('field', 'entity')
  get value(): SupportedValue | Array<SupportedValue> {
    const { entity, field } = this;
    const fieldName = field.fieldNameAlias || field.fieldName;
    return get(entity, fieldName as keyof DataModelEntity);
  }
}
