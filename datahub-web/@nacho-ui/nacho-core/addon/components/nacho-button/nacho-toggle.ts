import Component from '@glimmer/component';
import { action } from '@ember/object';

export const baseToggleClass = 'nacho-toggle';

export interface INachoToggleArgs<T, K> {
  /**
   * Passed in action that will be called upon the push of a side of the toggle and is intended to
   * switch the active value to whichever side is being clicked
   */
  onChange: (value: T | K) => void;

  /**
   * The current value of the toggle. This represents which of the two values presented in each option is the
   * value that is currently active. Types T, K represent the typing of either values
   */
  value?: T | K;

  /**
   * The value associated with the left option for the toggle
   */
  leftOptionValue?: T;

  /**
   * The label for the left option of the toggle
   */
  leftOptionText?: string;

  /**
   * The value associated with the right option for the toggle
   */
  rightOptionValue?: K;

  /**
   * The label for the right option fo the toggle
   */
  rightOptionText?: string;
}

/**
 * This toggle component will switch between two states or values, and call a function that passes in the
 * value being toggled to so that the consuming parent component will handle the interaction
 *
 * @example
 * {{nacho-toggle
 *   value=someValue
 *   leftOptionValue=someValue
 *   leftOptionText="Pikachu"
 *   rightOptionValue=someValue
 *   rightOptionText="Eevee"
 * }}
 */
export default class NachoToggle<T, K> extends Component<INachoToggleArgs<T, K>> {
  /**
   * Attach this base class to the component for more easy access in the component and increased
   * maintainability
   */
  baseToggleClass = baseToggleClass;

  /**
   * Action triggered by the user clicking on either side of the toggle. Whichever side is clicked
   * will determine the value passed into this action handler
   * @param {T | K} value - the value associated with either the right or left side of the toggle,
   *  whichever one was clicked by the user
   */
  @action
  onValueChange(value: T | K): void {
    typeof this.args.onChange === 'function' && this.args.onChange(value);
  }
}
