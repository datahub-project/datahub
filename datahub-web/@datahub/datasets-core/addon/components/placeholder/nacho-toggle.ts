import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/placeholder/nacho-toggle';
import { layout, classNames } from '@ember-decorators/component';
import { noop } from 'lodash';
import { action } from '@ember/object';

// TODO: [META-8566] This is a placeholder/test run for a nacho toggle component. If we like it as a standalone,
// it will be migrated to @nacho-ui and will replace this version

/**
 * This toggle component will switch between two states or values, and call a function that passes in the
 * value being toggled to so that the consuming parent component will handle the interaction
 *
 * @example
 * {{placeholder/nacho-toggle
 *   value=someValue
 *   leftOptionValue=someValue
 *   leftOptionText="Pikachu"
 *   rightOptionValue=someValue
 *   rightOptionText="Eevee"
 * }}
 */
@layout(template)
@classNames('nacho-toggle')
export default class PlaceholderNachoToggle<T, K> extends Component {
  /**
   * The current value of the toggle. This represents which of the two values presented in each option is the
   * value that is currently active. Types T, K represent the typing of either values
   * @type {T | K}
   */
  value?: T | K;

  /**
   * The value associated with the left option for the toggle
   * @type {T}
   */
  leftOptionValue?: T;

  /**
   * The label for the left option of the toggle
   * @type {string}
   */
  leftOptionText?: string;

  /**
   * The value associated with the right option for the toggle
   * @type {K}
   */
  rightOptionValue?: K;

  /**
   * The label for the right option fo the toggle
   */
  rightOptionText?: string;

  /**
   * Passed in action that will be called upon the push of a side of the toggle and is intended to
   * switch the active value to whichever side is being clicked
   * @type {(value: T | K) => void}
   */
  onChange: (value: T | K) => void = noop;

  /**
   * Action triggered by the user clicking on either side of the toggle. Whichever side is clicked
   * will determine the value passed into this action handler
   * @param {T | K} value - the value associated with either the right or left side of the toggle,
   *  whichever one was clicked by the user
   */
  @action
  onValueChange(value: T | K): void {
    this.onChange(value);
  }
}
