import Component from '@ember/component';
import { set } from '@ember/object';
import { INachoDropdownOption } from '@nacho-ui/core/types/nacho-dropdown';

import { action } from '@ember/object';
import { TaskInstance, task, timeout } from 'ember-concurrency';
import { classNames } from '@ember-decorators/component';
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

@classNames('nacho-drop-down')
export default class HoverDropDown<T> extends Component {
  /**
   * Toggle state of the drop-down
   * @type {boolean}
   * @memberof HoverDropDown
   */
  isExpanded = false;

  /**
   * Lists the options for the drop down content
   * @type {Array<INachoDropdownOption<T>>}
   * @memberof HoverDropDown
   */
  dropDownItems: Array<INachoDropdownOption<T>> = [];

  /**
   * References the currently selected drop-down option
   * @type {INachoDropdownOption<T>}
   * @memberof HoverDropDown
   */
  selectedDropDown: INachoDropdownOption<T>;

  /**
   * External action invoked when a drop-down option is selected
   * @memberof HoverDropDown
   */
  onSelect: (selected: INachoDropdownOption<T>) => void;

  /**
   * References the most recent TaskInstance to hide the drop-down options
   * @type {TaskInstance<Promise<void>>}
   * @memberof HoverDropDown
   */
  mostRecentHideTask: TaskInstance<Promise<void>>;

  /**
   * Task triggers the rendering of the list of drop-down options
   * @memberof HoverDropDown
   */
  @task(function*(this: HoverDropDown<T>, dd: IShowDropdownParams): IterableIterator<void> {
    set(this, 'isExpanded', true);
    dd.actions.open();
  })
  showDropDownTask!: ETask<void, IShowDropdownParams>;

  /**
   * Task triggers the occluding of the list of drop-down options
   * @type TaskProperty<void> & {
    perform: (a?: {
        actions: {
            open: () => void;
        };
    } | undefined) => TaskInstance<void>}
    * @memberof HoverDropDown
   */
  @task(function*(this: HoverDropDown<T>, dd: IHideDropdownParams): IterableIterator<Promise<void>> {
    set(this, 'isExpanded', false);
    yield timeout(200);
    dd.actions.close();
  })
  hideDropDownTask!: ETask<Promise<void>, IHideDropdownParams>;

  /**
   * Action handler to prevent bubbling DOM event action
   * @returns
   * @memberof HoverDropDown
   */
  @action
  prevent(): false {
    return false;
  }

  /**
   * Handles the DOM onmouseenter event to show list of drop-down options
   * @memberof HoverDropDown
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
   * @memberof HoverDropDown
   */
  @action
  hideDropDown(dd: IHideDropdownParams): void {
    set(this, 'mostRecentHideTask', this.hideDropDownTask.perform(dd));
  }

  /**
   * Invokes the external action with a reference to the selected drop-down option,
   * and sets a reference on this
   * @param {INachoDropdownOption<T>} selected the selected drop-down option, an instance of INachoDropdownOption<T>
   * @memberof HoverDropDown
   */
  @action
  onDropDownSelect(selected: INachoDropdownOption<T>): void {
    if (typeof this.onSelect === 'function') {
      this.onSelect(set(this, 'selectedDropDown', selected));
    }
  }
}
