import Component from '@ember/component';
import { action } from '@ember/object';
import { inject as service } from '@ember/service';
import HotKeys from 'datahub-web/services/hot-keys';

// Top level component class selector
const emberAppElementSelector = '.ember-application';

/**
 * GlobalHotkeys component provides a way to listen for registered keypress events on the DOM by handling the binding or unbinding of listener
 * event
 * @export
 * @class GlobalHotkeys
 * @extends {Component}
 */
export default class GlobalHotkeys extends Component {
  /**
   * A set of DOM keyup event targets for which the hotkey listener will be ignored. Ineligible targets are not useful for the hotkey feature, or
   * conflict with the default behavior of the target element for example input elements
   */
  inEligibleTargets = new Set(['INPUT', 'TEXTAREA']);

  /**
   * Service that assists with actually triggering the actions tied to a particular
   * target hotkey
   */
  @service
  hotKeys: HotKeys;

  /**
   * Returns true if target exists, is not an input, and is not an editable div
   * @param {Element} target - target element
   */
  isEligibleTarget(target: Element): boolean {
    return (
      !this.inEligibleTargets.has(target.tagName) &&
      !(target.tagName === 'DIV' && target.attributes.getNamedItem('contenteditable'))
    );
  }

  /**
   * Method for handling the global keyup.
   * @param {KeyboardEvent} e - KeyboardEvent triggered by user input
   */
  @action
  onKeyUp(e: KeyboardEvent): void {
    const target = e.target as Element | null;

    if (target && this.isEligibleTarget(target)) {
      this.hotKeys.applyKeyMapping(e.keyCode);
    }
  }

  /**
   * On DOM insertion, apply handler for keyup events
   */
  didInsertElement(): void {
    const app = document.querySelector(emberAppElementSelector);

    if (app) {
      app.addEventListener('keyup', this.onKeyUp);
    }
  }

  /**
   * On component destruction, remove hot-keys event handler
   */
  willDestroyElement(): void {
    const app = document.querySelector(emberAppElementSelector);

    if (app) {
      app.removeEventListener('keyup', this.onKeyUp);
    }
  }
}
