// TODO: META-8742 Add this as a component in the Nacho Component library as part of NachoHoverDropdown
import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/nacho-hover-dropdown/dropdown-option';

import { INachoDropdownOption } from '@nacho-ui/core/types/nacho-dropdown';

import { tagName, classNames, className, layout } from '@ember-decorators/component';
import { equal } from '@ember/object/computed';
import { noop } from 'lodash';

@tagName('li')
@layout(template)
@classNames('nacho-drop-down__options__option')
export default class NachoHoverDropdownDropdownOption<T> extends Component {
  /**
   * External action invoked when a INachoDropdownOption<T> instance is selected
   */
  onSelect: (option: INachoDropdownOption<T>) => void = noop;

  /**
   * Computed flag indicating if this option is an equal reference to the selectedOption
   */
  @className('nacho-drop-down__options__option--selected')
  @equal('option', 'selectedOption')
  isSelected?: boolean;

  /**
   * References the drop-down option passed in to this component
   */
  option?: INachoDropdownOption<T>;

  /**
   * References the selected drop-down option to determine is this option is the selected value
   */
  selectedOption?: INachoDropdownOption<T>;

  /**
   * Invokes the external action onSelect when this component's click event is triggered
   */
  click(): void {
    this.option && this.onSelect(this.option);
  }
}
