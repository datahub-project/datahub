import Component from '@ember/component';
import { INachoDropdownOption } from '@nacho-ui/core/types/nacho-dropdown';
import { tagName, classNames, className } from '@ember-decorators/component';
import { equal } from '@ember/object/computed';
import { noop } from 'lodash';

@tagName('li')
@classNames('nacho-drop-down__options__option')
export default class NachoDropDownOption<T> extends Component {
  /**
   * External action invoked when a INachoDropdownOption<T> instance is selected
   * @memberof NachoDropDownOption
   */
  onSelect: (option: INachoDropdownOption<T>) => void = noop;

  /**
   * Computed flag indicating if this option is referentially equal to the selectedOption
   * @type {boolean}
   * @memberof NachoDropDownOption
   */
  @className('nacho-drop-down__options__option--selected')
  @equal('option', 'selectedOption')
  isSelected: boolean;

  /**
   * @template T
   * References the drop-down option passed in to this
   * @memberof NachoDropDownOption
   */
  option: INachoDropdownOption<T>;

  /**
   * @template T
   * References the selected drop-down option
   * @memberof NachoDropDownOption
   */
  selectedOption: INachoDropdownOption<T>;

  /**
   * Invokes the external action onSelect when this component's click event is triggered
   * @memberof NachoDropDownOption
   */
  click(): void {
    this.onSelect(this.option);
  }
}
