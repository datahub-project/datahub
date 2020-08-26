import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/more-info';
import { tagName, classNames, layout } from '@ember-decorators/component';

// Block class for the component
const baseClass = 'more-info';

/**
 * Linkable tooltip typically used to guide the user to a help resource if a link is provided, or a tooltip otherwise
 * @export
 * @class MoreInfo
 * @extends {Component}
 */
@tagName('span')
@layout(template)
@classNames(baseClass)
export default class MoreInfo extends Component {
  /**
   * Component baseClass used as block for BEM
   */
  baseClass = baseClass;

  /**
   * Proxies to anchor element target attribute
   */
  target = '_blank';

  /**
   * Proxies to anchor element href attribute
   */
  link = '#';

  /**
   * Renders the tool tip component, if present
   */
  tooltip?: string;

  /**
   * Disables DOM event propagation
   */
  click = (): false => false;
}
