// TODO: META-8742 Add this as a component in the Nacho Component library NachoHoverDropdown has a behavior that's different from the basic dropdown
// Options are presented on hover and dropdown state is retained till mouse exit
// A list of actions would be the typical use case rather than a list of selection choices
import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/nacho-hover-dropdown';
import { set } from '@ember/object';
import { TaskInstance, timeout, task } from 'ember-concurrency';
import { action } from '@ember/object';
import { classNames, layout } from '@ember-decorators/component';
import { noop } from 'lodash';
import { INachoDropdownOption } from '@nacho-ui/core/types/nacho-dropdown';
import { ETask } from '@datahub/utils/types/concurrency';

/**
 * Params to show dropdown
 */
interface IShowDropdownParams {
  actions: { open: () => void };
}

/**
 * Params to hide dropdown
 */
interface IHideDropdownParams {
  actions: { close: () => void };
}

@layout(template)
@classNames('nacho-drop-down')
export default class NachoHoverDropdown<T> extends Component {
  /**
   * Desired trigger class
   */
  triggerClass?: string;

  /**
   * Toggle state of the drop-down
   * @type {boolean}
   */
  isExpanded = false;

  /**
   * Lists the options for the drop down content
   */
  dropDownItems: Array<INachoDropdownOption<T>> = [];

  /**
   * Optionally references the currently selected drop-down option if supported by caller use case
   * This value is not automatically populated when the user selects an option, this is left up to the
   * caller to update the component attribute externally
   */
  selectedDropDown?: INachoDropdownOption<T>;

  /**
   * External action invoked when a drop-down option is selected
   */
  onSelect: (selected: INachoDropdownOption<T>) => void = noop;

  /**
   * References the most recent TaskInstance to hide the drop-down options
   */
  mostRecentHideTask?: TaskInstance<Promise<void> | void>;

  /**
   * Task triggers the rendering of the list of drop-down options
   */
  @task(function*(this: NachoHoverDropdown<T>, dd: IShowDropdownParams): IterableIterator<void> {
    set(this, 'isExpanded', true);
    dd.actions.open();
  })
  showDropDownTask!: ETask<void, IShowDropdownParams>;

  /**
   * Task triggers the occluding of the list of drop-down options
   */
  @task(function*(this: NachoHoverDropdown<T>, dd: IHideDropdownParams): IterableIterator<Promise<void>> {
    set(this, 'isExpanded', false);
    yield timeout(200);
    dd.actions.close();
  })
  hideDropDownTask!: ETask<Promise<void>, IHideDropdownParams>;

  /**
   * Action handler to prevent bubbling DOM event action
   */
  @action
  prevent(): false {
    return false;
  }

  /**
   * Handles the DOM onmouseenter event to show list of drop-down options
   */
  @action
  showDropDown(dd: IShowDropdownParams): void {
    const lastHideTask = this.mostRecentHideTask;

    if (lastHideTask) {
      lastHideTask.cancel();
    }

    this.showDropDownTask.perform(dd);
  }

  /**
   * Handles the DOM event onmouseleave to hide the list of drop-down options and
   * stores a reference to the last invoked hideDropDownTask TaskInstance
   */
  @action
  hideDropDown(dd: IHideDropdownParams): void {
    set(this, 'mostRecentHideTask', this.hideDropDownTask.perform(dd));
  }

  /**
   * Invokes the external action with a reference to the selected drop-down option,
   * and sets a reference on this
   * @param {INachoDropdownOption<T>} selected the selected drop-down option, an instance of INachoDropdownOption<T>
   */
  @action
  onDropDownSelect(dd: IHideDropdownParams, selected: INachoDropdownOption<T>): void {
    if (typeof this.onSelect === 'function') {
      // selectedDropDown is not modified on dropdown class, this allows the external consumer to determine behavior after receiving selection
      // If the selectedDropDown option should be reflected on the dropdown, caller should update component attribute `selectedDropDown`
      this.onSelect(selected);

      this.hideDropDown(dd);
    }
  }
}
