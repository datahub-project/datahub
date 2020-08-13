// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/entity-page/entity-page-content/content-panel';
import { layout } from '@ember-decorators/component';
import { IContentPanelComponent } from '@datahub/data-models/types/entity/rendering/page-components';
import { assertComponentPropertyNotUndefined } from '@datahub/utils/decorators/assert';
import BasePageComponent from '@datahub/shared/components/entity-page/base-page-component';

/**
 * Content panel that will wrap a component with title, maybe a subheader, and some other common display info
 *
 * In the future this will also have a 'filter' dropdown to select which content to show, in a similar fashion than
 * tabs works.
 */
@layout(template)
export default class EntityPageContentContentPanel<E> extends BasePageComponent {
  /**
   * See documentation in the interface IContentPanelComponent.
   */
  @assertComponentPropertyNotUndefined
  options!: IContentPanelComponent<E>['options'];
}
