import TextField from '@ember/component/text-field';

export default class extends TextField {
  /**
   * Prevents click event bubbling
   */
  click: () => false;
}
