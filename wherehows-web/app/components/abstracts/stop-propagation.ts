import Component from '@ember/component';

/**
 * Defining our stop propagation here for DRY-ness
 * @param {KeyboardEvent} e - keyboard keyup trigger
 * @returns {boolean}
 */
function stopPropagation(e: KeyboardEvent): boolean {
  e.stopImmediatePropagation();
  return false;
}

/**
 * StopPropagationBase component can be extended by another component to capture event propagation
 * or wrap around template blocks.
 * @example
 * Usage case #1
 * ```
 * import StopPropagationBase from 'wherehows-web/abstracts/stop-propagation'
 *
 * export default class YourComponent extends StopPropagationBase { ... }
 * ```
 *
 * Usage case #2
 * This component can wrap around a template block. If there is a case where we do not want the
 * wrapper to have its own element in order to reduce DOM complexity, we can use also set the
 * component to be tagless.
 * ```
 * {{#abstracts/stop-propagation tagName="" as |stopPropagation|}}
 *   <input with keyup events onkeyup={{stopPropagation}}/>
 * {{/abstracts/stop-propagation}}
 * ```
 */
export default class StopPropagationBase extends Component {
  /**
   * Prevents keys in this component from propagating to the global level and unnecessarily calling
   * the global keyup listener. Useful when the user is expected to type a lot within the bounds of
   * a particular component
   * @param {KeyboardEvent} e - passed in by keyboard key up trigger
   */
  keyUp = this.tagName === '' ? undefined : stopPropagation;

  /**
   * Given to the template wrapper div tag in case we have a tagless component wrapper and keyUp()
   * cannot be defined as a result
   * @param {KeyboardEvent} e - passed in by keyboard key up trigger
   */
  stopPropagation = stopPropagation;
}
