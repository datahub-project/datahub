import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../templates/components/nacho-dropdown-basic';
import { action, computed } from '@ember/object';
import { classNames } from '@ember-decorators/component';
import { NachoDropdownOptions } from '@nacho-ui/dropdown/types/nacho-dropdown';

@classNames('nacho-dropdown', 'nacho-dropdown--basic')
export default class NachoDropdownBasic<T> extends Component {
  layout = layout;

  /**
   * Passed in by consuming parent component, should be a list of dropdown options to populate
   * @type {NachoDropdownOptions<T>}
   */
  options!: NachoDropdownOptions<T>;

  /**
   * Currently selected value, passed in by a consuming parent componetn. Should be able to match
   * up with one of the value properties of our options
   * @type {T}
   */
  selected!: T;

  /**
   * Whether the dropdown should be disabled. Passed in from consuming parent.
   * @type {boolean}
   */
  disabled!: boolean;

  /**
   * Passed in from consuming parent, is a handler for when the user selects an item from the
   * dropdown
   * @param {T} selected - the new selection item's value
   */
  selectionDidChange!: (selected: T) => void;

  @computed('options', 'selected')
  get renderedOptions(): NachoDropdownBasic<T>['options'] {
    const options: NachoDropdownBasic<T>['options'] = this.options || [];
    const selected = this.selected;

    return options.map(option => ({ ...option, isSelected: option.value === selected }));
  }

  constructor() {
    // eslint-disable-next-line prefer-rest-params
    super(...arguments);

    // Setting default values we need
    typeof this.selectionDidChange === 'function' || (this.selectionDidChange = (): void => {});
  }

  /**
   * Triggered by user action, passes up the handling to the consuming component.
   * @param selected - the selected item by the user's value
   */
  @action
  onSelectionChange(selected: T): void {
    this.selectionDidChange(selected);
  }
}
