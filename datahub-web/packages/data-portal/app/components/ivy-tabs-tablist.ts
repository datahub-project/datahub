import IvyTabsTablistComponent from 'ivy-tabs/components/ivy-tabs-tablist';
import { noop } from 'lodash';
import { scheduleOnce } from '@ember/runloop';
import { Keyboard } from 'datahub-web/constants/keyboard';

export default IvyTabsTablistComponent.extend({
  /**
   * Overwrites the ofiginal ivy-tabs-tablist component's on('keydown') method, which lets the
   * user navigate through the tabs using the arrow keys. As we don't want up and down arrows
   * to interfere with page scroll, we change this to a noop function.
   */
  navigateOnKeyDown: noop,

  /**
   * Using the more modern approach to handling component events, we include the functionality
   * to scroll between tabs with the left and right arrow keys. This way, the user can scroll
   * between tabs with the left/right keys and use the up/down keys to scroll the page, without
   * interruption
   */
  keyDown(event: KeyboardEvent) {
    switch (event.keyCode) {
      case Keyboard.ArrowLeft:
        this.selectPreviousTab();
        break;
      case Keyboard.ArrowRight:
        this.selectNextTab();
        break;
      default:
        return;
    }

    event.preventDefault();
    scheduleOnce('afterRender', this, this.focusSelectedTab);
  }
});
