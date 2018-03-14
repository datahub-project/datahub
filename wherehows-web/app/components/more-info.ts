import Component from '@ember/component';

export default class MoreInfo extends Component {
  tagName = 'span';

  classNames = ['more-info'];

  /**
   * Proxies to anchor element target attribute
   * @type {string}
   * @memberOf MoreInfo
   */
  target: string;

  /**
   * Proxies to anchor element href attribute
   * @type {string}
   * @memberOf MoreInfo
   */
  link: string;

  /**
   * Renders the tool tip component, if present
   * @type {string}
   * @memberOf MoreInfo
   */
  tooltip: string;

  constructor() {
    super(...arguments);

    this.target || (this.target = '_blank');
    this.link || (this.link = '#');
  }

  /**
   * Disables DOM event propagation
   * @return {boolean}
   */
  click = () => false;
}
