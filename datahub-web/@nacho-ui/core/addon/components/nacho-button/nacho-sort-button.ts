import Component from '@glimmer/component';
import { set, action } from '@ember/object';
import { computed } from '@ember/object';
import { assert } from '@ember/debug';

/**
 * Defines the two options for sort direction, used to test in the template which direction we
 * are currently sorting in, which in turn specifies the icon directions
 */
export enum SortDirection {
  ASCEND = 'asc',
  DESCEND = 'desc'
}

/**
 * Defines the class specifically for this button
 */
export const buttonClass = 'nacho-sort-button';

/**
 * Helper function that can be used by consuming components that allows us to cycle through sorting
 * scenarios on the consuming component by providing the key for isSorting and sortDirection and
 * allowing us to handle the mutation of those properties accordingly from
 * no sort => sort ascending => sort descending
 * @param {T} component - the consuming component
 * @param {string} isSortingKey - key for the property on the consuming component that says whether
 *  we are currently sorting
 * @param {string} sortDirectionKey - the key for the property on the consuming component that
 *  tells us in which direction oare we sorting
 */
export function cycleSorting<T>(component: T, isSortingKey: keyof T, sortDirectionKey: keyof T): void {
  assert(
    'cycleSorting: object[isSortingKey] provided should evaluate to a boolean',
    typeof component[isSortingKey] === 'boolean'
  );

  assert(
    'cycleSorting: sort direction should be included in enum SortDirection',
    typeof component[sortDirectionKey] === 'string'
  );

  const isSorting = (component[isSortingKey] as unknown) as boolean;
  const sortDirection = (component[sortDirectionKey] as unknown) as SortDirection;

  const setKeyOnComponent = (key: keyof T, value: unknown): typeof value => set(component, key, value as T[keyof T]);

  if (!isSorting) {
    setKeyOnComponent(isSortingKey, true);
    setKeyOnComponent(sortDirectionKey, SortDirection.ASCEND);
  } else {
    if (sortDirection === SortDirection.ASCEND) {
      setKeyOnComponent(sortDirectionKey, SortDirection.DESCEND);
    } else {
      setKeyOnComponent(isSortingKey, false);
    }
  }
}

/**
 * The NachoSortButton component is used when we want to display a button option to sort some list
 * that has been associated with the button
 *
 * @example
 * {{nacho-sort-button
 *   isSorting=isSortingABoolean
 *   sortDirection=sortDirectionAString
 *   sortValue="pokemon"
 *   class="test-sort-button"
 *   baseClass="test-sort-button"
 *   onChange=(action "onChangeSortProperty")
 * }}
 */
export default class NachoSortButton extends Component<{
  /**
   * Optional parameter where we can apply a base class to the icon so that its class becomes
   * "{{baseClass}}__icon"
   */
  baseClass?: string;

  /**
   * Passed in parameter that flags whether or not we are in a sorting state
   */
  isSorting?: boolean;

  /**
   * Passed in parameter that determines our sort direction (ascending or descending), only
   * applicable if we are currently in a sorting state
   */
  sortDirection?: SortDirection;

  /**
   * Passed in value that gets passed back to the onChange function when the sort button is
   * clicked. This allows the consuming component to use this value in any way they wish
   * in the consuming function. For example, it could allow a user to re-use the same action
   * for multiple sort buttons differentiating which was clicked by a certain value
   */
  sortValue?: unknown;

  /**
   * Passed in handler for the click of the sort button. Allows the consuming component to
   * really handle what they want to do upon changing of the sort
   */
  onChange?: (value: unknown) => void;
}> {
  /**
   * Including the SortDirection enum as a property on the component to easily access values in
   * the template
   */
  sortDirections: typeof SortDirection = SortDirection;

  /**
   * Including the button class as a property on the component to easily and consistently access
   * values in the template and make it more maintainable
   */
  buttonClass: string = buttonClass;

  /**
   * If we have a baseClass, we want to create a corresponding class for the icon below to
   * allow easy BEM selection for custom CSS overrides
   * @type {string}
   */
  @computed('args.baseClass')
  get iconClass(): string {
    const { baseClass } = this.args;
    let iconClass = `${buttonClass}__icon`;

    if (typeof baseClass === 'string' && baseClass) {
      iconClass += ` ${baseClass}__icon`;
    }

    return iconClass;
  }

  /**
   * Click handler for the overall button component. Defers to the parent for actual effects
   */
  @action
  onClickButton(): void {
    typeof this.args.onChange === 'function' && this.args.onChange(this.args.sortValue);
  }
}
