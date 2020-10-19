import Component from '@glimmer/component';
import { action, set, setProperties } from '@ember/object';
import { INachoDropdownOption } from '@nacho-ui/core/types/nacho-dropdown';
import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';
import { reads } from '@ember/object/computed';
import {
  IContentPanelWithToggle,
  IContentPanelWithToggleItem
} from '@datahub/data-models/types/entity/rendering/page-components';

/**
 * Args required for content-panel-with-toogle
 */
export interface IEntityPageContentContentPanelWithToggleArgs {
  // Options will be wrapped into this options object
  options: IContentPanelWithToggle['options'];
}

/**
 * Content panel that will have a toggle to switch between contents
 *
 * This component accept a dropdown with label to show in the toggle. Also it
 * contains a content component that will be rendered once the option is selected.
 * A toolbar component can be passed along with the content component if needed.
 *
 * Comunication between both will be done using a state (redux like). Child will trigger
 * onStateChanged once they require to change the state. This new state will be propagated
 * to other children. Make sure state is not mutated, so every state change results in a new
 * state object.
 */
export default class EntityPageContentContentPanelWithToggle<State> extends Component<
  IEntityPageContentContentPanelWithToggleArgs
> {
  /**
   * Dropdown option selected
   */
  optionSelected: INachoDropdownOption<IContentPanelWithToggleItem> = this.args.options.dropDownItems[0];

  /**
   * Current shared state
   */
  state?: State;

  /**
   * Shared state before current
   */
  lastState?: State;

  /**
   * Alias for content component
   */
  @reads('optionSelected.value.contentComponent')
  contentComponent!: IDynamicComponent;

  /**
   * Alias for toobar component
   */
  @reads('optionSelected.value.toolbarComponent')
  toolbarComponent?: IDynamicComponent;

  /**
   * Handle when dropdown option is selected
   * @param option Option to be selected
   */
  @action
  onSelect(option: INachoDropdownOption<IContentPanelWithToggleItem>): void {
    set(this, 'optionSelected', option);
  }

  /**
   * Handle when state is changed
   * @param state New state
   */
  @action
  onStateChanged(state: State): void {
    setProperties(this, {
      lastState: this.state,
      state: state
    });
  }
}
