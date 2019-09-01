import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../templates/components/more-info';
import { tagName, classNames } from '@ember-decorators/component';

// TODO: [META-8255] This is a copy of the component from data-portal and should be migrated to a centralized addon
// to share between our other addons
@tagName('span')
@classNames('more-info')
export default class MoreInfo extends Component {
  layout = layout;

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
