import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/forms/action-drawer';
import { layout, tagName, classNames, className } from '@ember-decorators/component';
import { action } from '@ember/object';

const baseClass = 'form-action-drawer';

/**
 * Shareable contextual component <Forms::ActionDrawer> renders a fixed positioned bar at the bottom of the screen.
 * Useful for forms / editable views that need to show a summary of user modifications.
 * This component is intentionally bare to allow for maximum flexibility with myriad use cases.
 *
 * Exposes an action for toggling the drawer and the flag indicating the drawer state
 * Sample usage:
 * <Forms::ActionDrawer as |drawer|>
 *    <MyComponent
 *      @isVisible={{drawer.isDrawerOpen}}
 *      @onShow={{drawer.onDrawerToggle}}
 *     />
 * </Forms::ActionDrawer>
 * @export
 * @class FormsActionDrawer
 * @extends {Component}
 */
@layout(template)
@tagName('section')
@classNames(baseClass)
export default class FormsActionDrawer extends Component {
  /**
   * Component CSS selector
   */
  baseClass = baseClass;

  /**
   * Block modifier selector, applied based on the flag value
   */
  @className(`${baseClass}--open`, `${baseClass}--close`)
  readonly isExpanded = false;

  /**
   * Handles the toggling of the drawer state, driven by the flag isExpanded
   */
  @action
  onDrawerToggle(): void {
    this.toggleProperty('isExpanded');
  }
}
