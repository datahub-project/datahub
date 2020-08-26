import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/entity-page/entity-page-main';
import { layout, classNames } from '@ember-decorators/component';
import { expandOptionsAttribute } from '@datahub/utils/decorators/expand-options-attribute';
import { assertComponentPropertyNotUndefined } from '@datahub/utils/decorators/assert';
import { DataModelEntity } from '@datahub/data-models/constants/entity';

/**
 * Main generic entity page that is consumed in the generic tab entity route
 */
@layout(template)
@classNames('entity-page-main')
@expandOptionsAttribute()
export default class EntityPageMain extends Component {
  /**
   * URN for the entity to show
   */
  @assertComponentPropertyNotUndefined
  urn!: string;

  /**
   * Entity class for the entity to show
   */
  @assertComponentPropertyNotUndefined
  entityClass!: DataModelEntity;

  /**
   * Current tab selected to show
   */
  @assertComponentPropertyNotUndefined
  tabSelected!: string;
}
