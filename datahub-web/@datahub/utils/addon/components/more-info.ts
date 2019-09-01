import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/more-info';
import { tagName, classNames, layout } from '@ember-decorators/component';

@tagName('span')
@classNames('more-info')
@layout(template)
export default class MoreInfo extends Component {
  /**
   * Proxies to anchor element target attribute
   * @type {string}
   * @memberOf MoreInfo
   */
  target: string = '_blank';

  /**
   * Proxies to anchor element href attribute
   * @type {string}
   * @memberOf MoreInfo
   */
  link: string = '#';

  /**
   * Renders the tool tip component, if present
   * @type {string}
   * @memberOf MoreInfo
   */
  tooltip?: string;

  /**
   * Disables DOM event propagation
   * @return {boolean}
   */
  click = () => false;
}
