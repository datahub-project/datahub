import { get, getWithDefault, computed } from '@ember/object';
import ComputedProperty from '@ember/object/computed';
import RadioButton from 'ember-radio-button/components/radio-button';
import { action } from '@ember-decorators/object';

export default class RadioButtonComposer extends RadioButton.extend({
  // RadioButton is a fragment, to allow DOM events, override with a DOM element
  tagName: 'span'
}) {
  classNameBindings = ['_disabledClass', '_checkedClass'];

  /**
   * Specifies the className to be added to the component when the class has a disabled
   * property that evaluates to a truthy value
   * @type {string}
   * @memberof RadioButtonComposer
   */
  disabledClass: string;

  /**
   * Resolves the class name binding for a component instance that is disabled i.e.
   * disabled attribute is truthy
   * @type {ComputedProperty<string>}
   * @memberof RadioButtonComposer
   */
  _disabledClass: ComputedProperty<string> = computed('disabled', function(this: RadioButtonComposer): string {
    const disabledClass = getWithDefault(this, 'disabledClass', '');
    return get(this, 'disabled') ? disabledClass : '';
  });

  /**
   * Resolves the class name binding for a component instance this is checked,
   * returns the inherited checkedClass property on RadioButton component, or
   * the runtime attribute with the same name
   * @type {ComputedProperty<string>}
   * @memberof RadioButtonComposer
   */
  _checkedClass: ComputedProperty<string> = computed('checked', function(this: RadioButtonComposer): string {
    const checkedClass = get(this, 'checkedClass');
    return get(this, 'checked') ? checkedClass : '';
  });

  didReceiveAttrs() {
    this._super(...arguments);

    // ensures that the values a supplied at the component call site
    ['name', 'groupValue', 'value'].forEach(attr => {
      if (!(attr in this)) {
        throw new Error(`Attribute '${attr}' is required to be passed in when instantiating this component.`);
      }
    });
  }

  /**
   * Handles the mouseenter event on the component element and invokes
   * the external action if provided as an attribute
   */
  mouseEnter() {
    const onMouseEnter = get(this, 'onMouseEnter');
    if (typeof onMouseEnter === 'function') {
      onMouseEnter({ value: get(this, 'value') });
    }
  }

  /**
   * Handles the mouseleave event on the component element and invokes
   * the external action if provided as an attribute
   */
  mouseLeave() {
    const onMouseLeave = get(this, 'onMouseLeave');
    if (typeof onMouseLeave === 'function') {
      onMouseLeave({ value: get(this, 'value') });
    }
  }

  /**
   * Invokes the external changed action component attribute
   * using spread args due to TS/ember-decorator being unable to discriminate
   * action vs component attribute
   * @param _args
   */
  @action
  changed(..._args: Array<any>): void {
    const closureAction = get(this, 'changed');

    if (typeof closureAction === 'function') {
      return closureAction(...arguments);
    }
  }
}
