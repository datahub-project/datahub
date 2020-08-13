import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/nacho/array-link-cell';
import { tagName, layout } from '@ember-decorators/component';
import { assertComponentPropertyNotUndefined } from '@datahub/utils/decorators/assert';
import { IDynamicLinkNode } from '@datahub/utils/types/vendor/dynamic-link';

/**
 * Renders a cell that shows a list of links
 *
 * Because NachoTable does not support dynamic components with options bag
 * we need to create this wrapper that suits nacho-table needs to render links
 */
@layout(template)
@tagName('td')
export default class NachoArrayLinkCell extends Component {
  /**
   * Value of the field we are trying to render
   */
  @assertComponentPropertyNotUndefined
  field: unknown | IDynamicLinkNode<unknown, unknown>;
}
