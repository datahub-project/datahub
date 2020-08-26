// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/entity-page/layouts/linear-layout';
import { layout } from '@ember-decorators/component';
import { ILinearLayoutComponent } from '@datahub/data-models/types/entity/rendering/page-components';
import BasePageComponent from '@datahub/shared/components/entity-page/base-page-component';

/**
 * It places subcomponents as list of components, one after the other
 */
@layout(template)
export default class LayoutsLinearLayout<E> extends BasePageComponent {
  /**
   * See ILinearLayoutComponent
   */
  options!: ILinearLayoutComponent<E>['options'];
}
