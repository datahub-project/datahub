import RadioButton from 'ember-radio-button/components/radio-button';
import { action, computed } from '@ember/object';
import { className, tagName } from '@ember-decorators/component';

// RadioButton is a fragment, to allow DOM events, override with a DOM element
@tagName('span')
export default class RadioButtonComposer<T> extends RadioButton {
  /**
   * Specifies the className to be added to the component when the class has a disabled
   * property that evaluates to a truthy value
   * @type {string}
   * @memberof RadioButtonComposer
   */
  disabledClass = '';

  /**
   * Resolves the class name binding for a component instance that is disabled i.e.
   * disabled attribute is truthy
   * @type {string}
   * @memberof RadioButtonComposer
   */
  @className
  @computed('disabled')
  get _disabledClass(): string {
    return this.disabled ? this.disabledClass : '';
  }

  /**
   * Resolves the class name binding for a component instance this is checked,
   * returns the inherited checkedClass property on RadioButton component, or
   * the runtime attribute with the same name
   * @type {string}
   * @memberof RadioButtonComposer
   */
  @className
  @computed('checked')
  get _checkedClass(): string {
    return this.checked ? this.checkedClass : '';
  }

  didReceiveAttrs(): void {
    super.didReceiveAttrs();

    // ensures that the values a supplied at the component call site
    ['name', 'groupValue', 'value'].forEach(attr => {
      if (!(attr in this)) {
        throw new Error(`Attribute '${attr}' is required to be passed in when instantiating this component.`);
      }
    });
  }

  didInsertElement(): void {
    super.didInsertElement();
    this.element.addEventListener('mouseenter', this.handleMouseEnter);
    this.element.addEventListener('mouseleave', this.handleMouseLeave);
  }

  willDestroyElement(): void {
    super.willDestroyElement();
    this.element.removeEventListener('mouseenter', this.handleMouseEnter);
    this.element.removeEventListener('mouseleave', this.handleMouseLeave);
  }

  /**
   * Handles the mouseenter event on the component element and invokes
   * the external action if provided as an attribute
   */
  @action
  handleMouseEnter(): void {
    const { onMouseEnter, value } = this;
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
    const { onMouseLeave, value } = this;
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
  click(): void {
    const { onclick, value } = this;

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
    const { onChange } = this;

    if (typeof onChange === 'function') {
      return onChange(...args);
    }
  }
}
