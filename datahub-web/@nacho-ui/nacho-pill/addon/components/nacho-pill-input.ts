// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/nacho-pill-input';
import { layout, classNames, className } from '@ember-decorators/component';
import { noop } from '@nacho-ui/core/utils/functions/noop';
import { action, set, computed, setProperties } from '@ember/object';
import BasePillComponent, { PillState } from '@nacho-ui/pill/components/nacho-pill';

export const baseInputPillClass = 'nacho-pill-input';

/**
 * The nacho pill input is used when you want to allow input of a set of tags.
 *
 * Expected behavior:
 * - If the tag already has a value, clicking on the X will trigger a function with the intention
 *   to delete the tag
 * - If the tag is an empty tag, clicking on the + will put us in "editing" mode to add a value
 * - While in editing mode, clicking the + again or pressing the enter key will create a new tag
 * - While in editing mode, pressing the tab key will create a new tag and also leave us in
 *   editing mode still to quickly add more tags
 * - While in editing mode, clicking away will cause the input pill to reset
 * - If typeahead is enabled then upon searching a dropdown will surface with the suggestions
 *
 * Refer to dummy application component <TestNachoPillInput> for suggested use of this component
 * @example
 * <NachoPillInput
 *  @value={{"string" || undefined}}
 *  @isTypeahead={{true || undefined}}
 *  @placeholder={{"string" || undefined}}
 *  @onComplete={{action onComplete}}
 *  @onDelete={{action onDelete}}
 *  @baseState={{PillState.Good}}
 *  @emptyState={{PillState.GoodInverse}}
 * />
 * </NachoPill::Input>
 */
@layout(template)
@classNames(baseInputPillClass)
export default class NachoPillInput<T> extends BasePillComponent {
  /**
   * Inclusion in component for easy access in the template
   */
  baseClass = baseInputPillClass;

  /**
   * Whether or not we are currently in an editing state
   */
  isEditing = false;

  /**
   * Optional attribute to set the max character length
   */
  maxlength?: number;

  /**
   * The value passed into this tag input as an expected string
   */
  value?: string;

  /**
   *  Whether or not we want to use nacho-pill-input as an input for a typeahead dropdown
   */
  isTypeahead = false;

  /**
   * This is expected to be updated based on the user's input into the pill during editing mode.
   * We don't connect it directly to the value parameter because we don't necessarily know if
   * it will connect to any property directly.
   */
  tagValue?: T;

  /**
   * Expected to be passed in, or will get a default value in the constructor, determines the
   * basic appearance of the pill when it is trying to display a value
   */
  baseState!: PillState;

  /**
   * Expected to be passed in, or will be assigned a default in the constructor, determines the
   * appearance of the pill when there is no value and the user should be prompted to enter one.
   * It is helpful to differentiate this from a normal pill so that the user knows that it is a
   * different expected behavior
   */
  emptyState!: PillState;

  /**
   * External action, meant to be triggered by the user upon one of many possible actions.
   * Ultimately, this confirms their choice to finalize their pill entry and add the typed in value
   * to somelist
   */
  onComplete: (value: string | T) => void = noop;

  /**
   * External action, meant to handle a user action expressing desire to delete the value
   * associated with the current pill input
   */
  onDelete: (value: string) => void = noop;

  /**
   * Determines the CSS classes that must be applied to the pill based on the determination of
   * various properties passed into this component. If there is a value active, then we define
   * the CSS from the "state" given, otherwise we define the appearance from the "empty state"
   */
  @className
  @computed('state', 'emptyState', 'value')
  get pillClasses(): string {
    const { state, emptyState, value } = this;
    return `nacho-pill--${value ? state : emptyState}`;
  }

  /**
   * Convenience method to trigger the creation of a new value (if a tag has been typed in) and
   * reset the current tag to having no value
   */
  createPillAndReset(): void {
    if (this.tagValue) {
      this.onComplete(this.tagValue);
      set(this, 'tagValue', undefined);
    }
  }

  /**
   * Takes advantage of component lifecycle to perform actions after each render loop
   */
  didRender(): void {
    super.didRender();
    // For user convenience, this next block of code is meant to auto focus the input element after
    // it has been placed into the DOM so that the user can start typing right away
    if (this.isEditing) {
      const input = this.element.getElementsByTagName('input')[0];
      input && input.focus();
    }
  }

  /**
   * Used to detect certain keypresses by the user, the primary goal of this action existing here
   * is to prevent the tab key from focusing some other element in the DOM. Instead, we want the
   * tab key to help the user quickly add multiple tags
   * @param {KeyboardEvent} keyEvent - event triggered by the keydown action, lets us know what
   *  the user has pressed
   */
  @action
  userKeyDown(keyEvent: KeyboardEvent): void {
    const KEYBOARD_TAB_KEY = 9;
    if (keyEvent.keyCode === KEYBOARD_TAB_KEY) {
      keyEvent.preventDefault();
      this.createPillAndReset();
    }
  }

  /**
   * If the user clicks away, we want to reset this pill, but not actually trigger the creation of
   * any new tags from it
   */
  @action
  userFocusedOut(event: MouseEvent): void {
    // element where the focus out happens
    const mainTarget = event.target as Node;
    // a secondary mouse event if any that happened alongside the focusOut. Eg : "onClick"
    const secondaryTarget = event.relatedTarget as Node;
    // if both targets are valid
    const isTargetValid = mainTarget && secondaryTarget;
    // if the targets are children of the current element in scope
    const isChildPresent = this.element.contains(mainTarget) && this.element.contains(secondaryTarget);

    // If focusOut does not stem from a sibling button then reset tagValue , else ignore and let secondaryTargetHandler take over the event propagation
    if (isTargetValid && !isChildPresent) {
      setProperties(this, { tagValue: undefined, isEditing: false });
    }
  }

  /**
   * Handler method for the PowerSelectTypeahead component, which dictates how we handle NachoPillInput when user focuses out.
   */
  @action
  onBlur(_dd: unknown | undefined, event: MouseEvent | undefined): void {
    // a secondary mouse event if any that happened alongside the focusOut. Eg : "onClick"
    const secondaryTarget = event?.relatedTarget as Node | undefined;

    // When clicked in a dropdown option, secondary target is null. We just want to take action when the
    // secondary target is defined and it is outside ourselves
    if (secondaryTarget && !this.element.contains(secondaryTarget)) {
      setProperties(this, { tagValue: undefined, isEditing: false });
    }
  }

  /**
   * Action that handles the basic user confirmation of their desire to finish editing and add the
   * tag they've created to whatever list provided the context for this pill and its siblings
   */
  @action
  userConfirmedEntry(selectedEntry: T): void {
    set(this, 'isEditing', false);
    if (selectedEntry) {
      set(this, 'tagValue', selectedEntry);
    }
    this.createPillAndReset();
  }

  /**
   * Action that handles the deletion action by the user for this tag. We give them the value to
   * help identify which tag is being removed
   */
  @action
  userDeletedTag(): void {
    const { value } = this;
    value && this.onDelete(value);
  }

  /**
   * This function handler exists as a convenience. So that the user doesn't have to click on the
   * tiny + symbol to start editing, if they click on the pill in general then we will toggle
   * editing if this pill is meant to be edited
   */
  click(): void {
    if (!this.value && !this.isEditing) {
      set(this, 'isEditing', true);
    }
  }

  constructor() {
    // @ts-ignore typing technically doesn't allow ...arguments because we are inheriting from
    // BasePillComponent but we need this for Ember.Component to function properly
    // eslint-disable-next-line prefer-rest-params
    super(...arguments);
    // Applying default here as the version of ember of this package is not high enough yet to be able
    // to assign default properties within the class field declaration itself
    this.state = typeof this.baseState === 'string' ? this.baseState : PillState.NeutralInverse;
    typeof this.emptyState === 'string' || (this.emptyState = PillState.Neutral);
  }
}
