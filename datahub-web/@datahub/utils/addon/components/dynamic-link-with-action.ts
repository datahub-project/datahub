// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/dynamic-link-with-action';
import { layout } from '@ember-decorators/component';
import DynamicLinkBase from 'dynamic-link/components/dynamic-link';

/**
 * The purpose of this component is to extend and override some of the original dynamic-link
 * component functionalities that use outdated methods and allow us to fully take advantage of
 * dynamically assigning actions on click
 */
@layout(template)
export default class DynamicLinkWithAction extends DynamicLinkBase {
  /**
   * Externally provided click action, optional because we only want to call an action if this is
   * a provided parameter
   */
  onClickAction?: () => void;

  /**
   * If an onClickAction parameter was provided, then run the action. Otherwise default to
   * original behavior
   * @override
   */
  performAction(): void {
    const { onClickAction } = this;
    onClickAction ? onClickAction() : super.performAction();
  }

  /**
   * Refactors the click event from the base to look for an "onClickAction" instead of some action
   * string.
   * @param {MouseEvent} event - the event triggering the click function
   * @override
   */
  // From original component:
  // Use prevent default to keep route transitions and actions from refreshing the page. Allows
  // click behavior to bubble up if allowed by the bubbles property
  click(event: MouseEvent): boolean {
    if (this.onClickAction && !(event.metaKey || event.ctrlKey)) {
      event.preventDefault();
      this.performAction();
      return this.bubbles !== false;
    }

    return super.click(event);
  }
}
