import Component from '@ember/component';
import { get, set } from '@ember/object';
import { task, TaskInstance, timeout } from 'ember-concurrency';
import { assert } from '@ember/debug';
import { IDropDownOption } from 'wherehows-web/typings/app/dataset-compliance';
import { action } from '@ember-decorators/object';

export default class HoverDropDown<T> extends Component {
  classNames = ['nacho-drop-down'];

  /**
   * Toggle state of the drop-down
   * @type {boolean}
   * @memberof HoverDropDown
   */
  isExpanded: boolean | void;

  /**
   * Lists the options for the drop down content
   * @type {Array<IDropDownOption<T>>}
   * @memberof HoverDropDown
   */
  dropDownItems: Array<IDropDownOption<T>>;

  /**
   * References the currently selected drop-down option
   * @type {IDropDownOption<T>}
   * @memberof HoverDropDown
   */
  selectedDropDown: IDropDownOption<T>;

  /**
   * External action invoked when a drop-down option is selected
   * @memberof HoverDropDown
   */
  onSelect: (selected: IDropDownOption<T>) => void;

  /**
   * References the most recent TaskInstance to hide the drop-down options
   * @type {TaskInstance<Promise<void>>}
   * @memberof HoverDropDown
   */
  mostRecentHideTask: TaskInstance<Promise<void>>;

  /**
   * Creates an instance of HoverDropDown.
   * @memberof HoverDropDown
   */
  constructor() {
    super(...arguments);

    const typeOfItems = typeof this.dropDownItems;
    assert(`Expected prop dropDownItems to be of type Array, got ${typeOfItems}`, Array.isArray(this.dropDownItems));
    // defaults the isExpanded flag to false
    typeof this.isExpanded === 'boolean' || (this.isExpanded = false);
  }

  /**
   * Task triggers the rendering of the list of drop-down options
   * @type TaskProperty<void> & {
    perform: (a?: {
        actions: {
            open: () => void;
        };
    } | undefined) => TaskInstance<void>}
   * @memberof HoverDropDown
   */
  showDropDownTask = task(function*(
    this: HoverDropDown<T>,
    dd: { actions: { open: () => void } }
  ): IterableIterator<void> {
    set(this, 'isExpanded', true);
    dd.actions.open();
  });

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
  hideDropDownTask = task(function*(
    this: HoverDropDown<T>,
    dd: { actions: { close: () => void } }
  ): IterableIterator<Promise<void>> {
    set(this, 'isExpanded', false);
    yield timeout(200);
    dd.actions.close();
  });

  /**
   * Action handler to prevent bubbling DOM event action
   * @returns
   * @memberof HoverDropDown
   */
  @action
  prevent() {
    return false;
  }

  /**
   * Handles the DOM onmouseenter event to show list of drop-down options
   * @memberof HoverDropDown
   */
  @action
  showDropDown() {
    const lastHideTask = get(this, 'mostRecentHideTask');

    if (lastHideTask) {
      lastHideTask.cancel();
    }

    get(this, 'showDropDownTask').perform(...arguments);
  }

  /**
   * Handles the DOM event onmouseleave to hide the list of drop-down options and
   * stores a reference to the last invoked hideDropDownTask TaskInstance
   * @memberof HoverDropDown
   */
  @action
  hideDropDown() {
    set(this, 'mostRecentHideTask', get(this, 'hideDropDownTask').perform(...arguments));
  }

  /**
   * Invokes the external action with a reference to the selected drop-down option,
   * and sets a reference on this
   * @param {IDropDownOption<T>} selected the selected drop-down option, an instance of IDropDownOption<T>
   * @memberof HoverDropDown
   */
  @action
  onDropDownSelect(selected: IDropDownOption<T>) {
    if (typeof this.onSelect === 'function') {
      this.onSelect(set(this, 'selectedDropDown', selected));
    }
  }
}
