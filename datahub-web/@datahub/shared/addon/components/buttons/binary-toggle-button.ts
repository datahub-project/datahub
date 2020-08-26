import Component from '@glimmer/component';
import { noop } from 'lodash';
import { action } from '@ember/object';

interface IButtonsBinaryToggleButtonArgs {
  // Click event handler for the affirmation button
  onAffirm?: () => void;
  // Click event handler for the deny button
  onDeny?: () => void;
  // Click event handler to reverse a previous affirmation or denial action
  onUndo?: () => unknown;
  // Current state of the clicked state button
  state?: State;
  // Text labels for each state intended to be rendered when the user has clicked a state
  stateLabels?: Record<State, string>;
  // Help / tooltip text for the affirmation button
  affirmTitle?: string;
  // Help / tooltip text for the deny button
  denyTitle?: string;
  // Flag indicating the affirm button should be disabled from interaction
  affirmDisabled?: boolean;
  // Flag indicating the deny button should be disabled from interaction
  denyDisabled?: boolean;
}

/**
 * State flags mapping to each button in the toggle
 * @export
 * @enum {string}
 */
export enum State {
  affirm = 'affirm',
  deny = 'deny'
}

// BEM block for the component
const baseClass = 'binary-toggle';

// BEM element selector for the component button element
export const buttonClass = `${baseClass}-button`;

/**
 * Groups two svg icon buttons as a binary operation between the two buttons and renders a button to
 * reverse the state change when the action buttons are interacted with
 * @export
 * @class ButtonsBinaryToggleButton
 * @extends {Component<IButtonsBinaryToggleButtonArgs>}
 */
export default class ButtonsBinaryToggleButton extends Component<IButtonsBinaryToggleButtonArgs> {
  /**
   * Component reference for template accessibility
   */
  baseClass = baseClass;

  /**
   * Button class is added to the nested button components
   */
  buttonClass = buttonClass;

  /**
   * Enum is partially applied to action when user click button in template to distinguish which button
   * was clicked while using the same click handler for both
   */
  State = State;

  /**
   * Resolves the value of the label text when the state of the toggled buttons has changed
   * @readonly
   */
  get stateLabel(): string | undefined {
    const { stateLabels, state } = this.args;

    return stateLabels && state ? stateLabels[state] : undefined;
  }

  /**
   * Optional class for the resolved state once either of the options has been clicked
   * @readonly
   */
  get stateClass(): string {
    const { state } = this.args;

    return state ? `${this.baseClass}__label--${state}` : '';
  }

  /**
   * Handle the click event for either button
   * @param {State} stateChange the state flag for the clicked button
   */
  @action
  onClick(stateChange: State): void {
    const { onAffirm = noop, onDeny = noop } = this.args;
    stateChange === State.affirm ? onAffirm() : onDeny();
  }

  /**
   * Allows the user to reverse the state the button may have transitioned to by invoking the
   * external action for undo if supplied
   */
  @action
  onReverse(): void {
    const { onUndo = noop } = this.args;
    onUndo();
  }
}
