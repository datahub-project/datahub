import Component from '@ember/component';

export default class StopPropagationBase extends Component {
  /**
   * Prevents keys in this component from propagating to the global level and unnecessarily calling
   * the global keyup listener. Useful when the user is expected to type a lot within the bounds of
   * a particular component
   * @param {KeyboardEvent} e - passed in by keyboard key up trigger
   */
  keyUp(e: KeyboardEvent) {
    e.stopImmediatePropagation();
  }
}
