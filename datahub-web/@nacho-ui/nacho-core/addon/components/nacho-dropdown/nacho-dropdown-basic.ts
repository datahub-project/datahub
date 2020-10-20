import Component from '@glimmer/component';
import { action, computed } from '@ember/object';
import { NachoDropdownOptions } from '@nacho-ui/core/types/nacho-dropdown';

interface INachoDropdownBasicArgs<T> {
  /**
   * Passed in id name
   */
  id?: string;

  /**
   * Passed in css class
   */
  class?: string;

  /**
   * Passed in by consuming parent component, should be a list of dropdown options to populate
   * @type {NachoDropdownOptions<T>}
   */
  options: NachoDropdownOptions<T>;

  /**
   * Currently selected value, passed in by a consuming parent componetn. Should be able to match
   * up with one of the value properties of our options
   * @type {T}
   */
  selected: T;

  /**
   * Whether the dropdown should be disabled. Passed in from consuming parent.
   * @type {boolean}
   */
  disabled: boolean;

  /**
   * Passed in from consuming parent, is a handler for when the user selects an item from the
   * dropdown
   * @param {T} selected - the new selection item's value
   */
  selectionDidChange: (selected: T) => void;
}

export default class NachoDropdownBasic<T> extends Component<INachoDropdownBasicArgs<T>> {
  @computed('args.{options,selected}')
  get renderedOptions(): INachoDropdownBasicArgs<T>['options'] {
    const options: INachoDropdownBasicArgs<T>['options'] = this.args.options || [];
    const selected = this.args.selected;

    return options.map(option => ({ ...option, isSelected: option.value === selected }));
  }

  /**
   * Triggered by user action, passes up the handling to the consuming component.
   * @param selected - the selected item by the user's value
   */
  @action
  onSelectionChange(selected: T): void {
    this.args.selectionDidChange && this.args.selectionDidChange(selected);
  }
}
