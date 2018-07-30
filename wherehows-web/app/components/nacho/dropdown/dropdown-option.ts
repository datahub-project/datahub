import Component from '@ember/component';
import { IDropDownOption } from 'wherehows-web/typings/app/dataset-compliance';
import { get, computed } from '@ember/object';

export default class NachoDropDownOption<T> extends Component {
  tagName = 'li';

  classNames = ['nacho-drop-down__options__option'];

  classNameBindings = ['isSelected:nacho-drop-down__options__option--selected'];

  /**
   * External action invoked when a IDropDownOption<T> instance is selected
   * @memberof NachoDropDownOption
   */
  onSelect: (option: IDropDownOption<T>) => void;

  /**
   * Computed flag indicating if this option is referentially equal to the selectedOption
   * @type {ComputedProperty<boolean>}
   * @memberof NachoDropDownOption
   */
  isSelected = computed('option', 'selectedOption', function(this: NachoDropDownOption<T>): boolean {
    return get(this, 'selectedOption') === get(this, 'option');
  });

  /**
   * References the drop-down option passed in to this
   * @type {IDropDownOption<T>}
   * @memberof NachoDropDownOption
   */
  option: IDropDownOption<T>;

  /**
   * References the selected drop-down option
   * @type {IDropDownOption<T>}
   * @memberof NachoDropDownOption
   */
  selectedOption: IDropDownOption<T>;

  /**
   * Invokes the external action onSelect when this component's click event is triggered
   * @memberof NachoDropDownOption
   */
  click() {
    this.onSelect(get(this, 'option'));
  }
}
