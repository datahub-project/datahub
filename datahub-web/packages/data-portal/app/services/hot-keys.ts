import Service from '@ember/service';
import { Keyboard } from 'datahub-web/constants/keyboard';
import { get } from '@ember/object';
import { noop } from 'lodash';

export default class HotKeys extends Service {
  /**
   * Used to map our various keycodes to methods that have been registered by various components
   * that incorporate this service
   * @type {Object<function>}
   */
  keyMappings: Partial<Record<Keyboard, () => void>> = {};

  /**
   * Called by various components, binds a keycode from a keyboard event to a specified action on
   * that particular component
   * @param keyCode - keycode given by the keyboard event that will triggered
   * @param action - function to bind to this key code
   */
  registerKeyMapping(keyCode: Keyboard, action: () => void): void {
    get(this, 'keyMappings')[keyCode] = action;
  }

  /**
   * In a situation where a component no longer should be calling an action for a specific hotkey,
   * this lets us clear that out
   * @param keyCode - keycode that has been registered
   */
  unregisterKeyMapping(keyCode: Keyboard): void {
    get(this, 'keyMappings')[keyCode] = noop;
  }

  /**
   * When the user has a keyup event, this will bubble up to our application body and trigger a
   * handler on our global-hotkeys component that handles these. That component then determines
   * if the key came from an elligible source for hotkeys and then calls this function to attempt
   * to call a mapped action to that key
   * @param keyCode - keycode tied to keyboard event that triggered this method
   */
  applyKeyMapping(keyCode: Keyboard): void {
    const action = get(this, 'keyMappings')[keyCode];
    action && action();
  }
}
