import { action, computed } from '@ember/object';
import Component from '@glimmer/component';
import { isEqual } from '@ember/utils';

// RadioButton is a fragment, to allow DOM events, override with a DOM element
export default class RadioButtonComposer<T> extends Component<{
  // ember radio button is not maintain anymore
  // extracted from https://github.com/yapplabs/ember-radio-button/blob/master/addon/components/radio-button.js
  value?: string; //- passed in, required, the value for this radio button
  groupValue?: string; //- passed in, required, the currently selected value
  disabled?: boolean;
  /**
   * Specifies the className to be added to the component when the class has a disabled
   * property that evaluates to a truthy value
   * @type {string}
   */
  disabledClass?: string;

  checkedClass?: string;
  onMouseEnter?: (arg: { value: string | undefined }) => void;
  onMouseLeave?: (arg: { value: string | undefined }) => void;
  onclick?: (arg: { value: string | undefined }) => void;
  onChange?: (...args: Array<unknown>) => void;
}> {
  @computed('args.{groupValue,value}')
  get checked(): boolean {
    return isEqual(this.args.groupValue, this.args.value);
  }

  /**
   * Resolves the class name binding for a component instance that is disabled i.e.
   * disabled attribute is truthy
   * @type {string}
   * @memberof RadioButtonComposer
   */
  @computed('args.{disabled,disabledClass}')
  get disabledClass(): string {
    return this.args.disabled ? this.args.disabledClass || 'disabled' : '';
  }

  /**
   * Resolves the class name binding for a component instance this is checked,
   * returns the inherited checkedClass property on RadioButton component, or
   * the runtime attribute with the same name
   * @type {string}
   * @memberof RadioButtonComposer
   */
  @computed('checked', 'args.checkedClass')
  get checkedClass(): string {
    return this.checked ? this.args.checkedClass || 'checked' : '';
  }

  @action
  bindEvents(element: HTMLElement): void {
    element.addEventListener('mouseenter', this.handleMouseEnter);
    element.addEventListener('mouseleave', this.handleMouseLeave);
    element.addEventListener('click', this.handleClick);
  }

  @action
  unBindEvents(element: HTMLElement): void {
    element.removeEventListener('mouseenter', this.handleMouseEnter);
    element.removeEventListener('mouseleave', this.handleMouseLeave);
    element.removeEventListener('click', this.handleClick);
  }

  /**
   * Handles the mouseenter event on the component element and invokes
   * the external action if provided as an attribute
   */
  @action
  handleMouseEnter(): void {
    const { onMouseEnter, value } = this.args;
    if (typeof onMouseEnter === 'function') {
      onMouseEnter({ value });
    }
  }

  /**
   * Handles the mouseleave event on the component element and invokes
   * the external action if provided as an attribute
   */
  @action
  handleMouseLeave(): void {
    const { onMouseLeave, value } = this.args;
    if (typeof onMouseLeave === 'function') {
      onMouseLeave({ value });
    }
  }

  /**
   * Typically the click event interaction on a radio button should be inconsequential, what should matter is the
   * change event. However, there are cases when this click event may be useful or be meaningful to capture. Hence,
   * if an onclick handler is provided it will be invoked
   * @memberof RadioButtonComposer
   */
  @action
  handleClick(): void {
    const { onclick, value } = this.args;

    typeof onclick === 'function' && onclick({ value });
  }

  /**
   * Invokes the external onChange action component attribute
   * using spread args due to TS/ember-decorator being unable to discriminate
   * action vs component attribute
   * @param args
   */
  @action
  changed(...args: Array<T>): void {
    const { onChange } = this.args;

    if (typeof onChange === 'function') {
      return onChange(...args);
    }
  }
}
