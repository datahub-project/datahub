import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../templates/components/expandable-content';
import { classNames } from '@ember-decorators/component';
import { action } from '@ember/object';

const baseComponentClass = 'expandable-content';

@classNames(`${baseComponentClass}__container`)
export default class ExpandableContent extends Component {
  layout = layout;

  /**
   * Leads to ease of use in the template level
   * @type {string}
   */
  baseComponentClass = baseComponentClass;

  /**
   * The expanded state of this component. When the user clicks on the button trigger, it'll toggle this
   * between expanded and unexpanded. Can optionally be passed in as a parameter, or defaulted to being
   * a local property
   * @type {boolean}
   */
  isExpanded = false;

  /**
   * Toggles the expanded state of this component, which will then reveal the yielded content in an
   * "expanded" window
   */
  @action
  toggleExpanded(): void {
    this.toggleProperty('isExpanded');
  }
}
