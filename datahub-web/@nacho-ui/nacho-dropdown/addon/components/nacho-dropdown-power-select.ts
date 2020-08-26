import NachoDropdownBasic from '@nacho-ui/dropdown/components/nacho-dropdown-basic';
// @ts-ignore: Ignore import of compiled template
import layout from '../templates/components/nacho-dropdown-power-select';
import { classNames } from '@ember-decorators/component';
import { computed, action } from '@ember/object';
import { INachoDropdownOption } from '@nacho-ui/dropdown/types/nacho-dropdown';

const baseClass = 'nacho-dropdown';

/**
 * Adds an option to the dropdown options to be recognized by the power select underlying component
 */
type PowerSelectOption<T> = INachoDropdownOption<T> & { disabled?: boolean };

/**
 * The NachoDropdownPowerSelect component is a wrapper around the ember-power-select addon's PowerSelect component.
 * While this component provides a very good general interface that accommodates a large number of uses cases, the
 * actual use cases that we use in our data applications are more specific and limited. Therefore, this wrapper
 * lets us deal less with the inner workings of the general component and exposes a limited number of parameters that
 * shortcut to the desired outcomes of the PowerSelect component for us. The intention is that using PowerSelect is
 * as easy as using any Nacho dropdown component
 */
@classNames(`${baseClass} ${baseClass}--basic ${baseClass}--deco`)
export default class NachoDropdownPowerSelect<T> extends NachoDropdownBasic<T> {
  layout = layout;

  /**
   * Adding base class to the component for easier access within the template
   */
  baseClass = baseClass;

  /**
   * Placeholder to be passed onto the power select component
   */
  placeholder?: string;

  /**
   * Takes the rendered options created by the basic dropdown component and appends with information necessary for
   * compatibility with the PowerSelect component
   */
  @computed('options')
  get renderedOptions(): Array<PowerSelectOption<T>> {
    return super.renderedOptions.map(renderedOption => ({
      ...renderedOption,
      disabled: renderedOption.isDisabled || renderedOption.isCategoryHeader
    }));
  }

  /**
   * Because power select uses equality of objects to determine the currently selected option instead of the actual
   * value within the option, this allows us to keep the nacho dropdown general interface the same by provides
   * compatibility with the power select interface
   */
  @computed('renderedOptions', 'selected')
  get selectedOption(): PowerSelectOption<T> {
    const { selected, renderedOptions } = this;
    return renderedOptions.filter(option => option.value === selected)[0];
  }

  /**
   * Since the value passed to this can be the entire option now and not just the value, this ensures that we are
   * passing the correct value back with this function consistent with nacho dropdown expectations
   * @param selection - potential option selected
   */
  @action
  onSelectionChange(selection: T | PowerSelectOption<T>): void {
    this.selectionDidChange((selection as PowerSelectOption<T>).value || (selection as T));
  }
}
