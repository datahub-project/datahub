import Component from '@ember/component';
import ComputedProperty from '@ember/object/computed';
import { computed, getProperties } from '@ember/object';
import noop from 'wherehows-web/utils/noop';
import { SuggestionIntent } from 'wherehows-web/constants';

/**
 * Describes the interface for the auto-suggest-action component
 * @interface IAutoSuggestAction
 */
interface IAutoSuggestAction {
  type: SuggestionIntent | void;
  action: Function;
  isAffirmative: ComputedProperty<boolean>;
}

export default Component.extend(<IAutoSuggestAction>{
  tagName: 'button',

  classNames: ['compliance-auto-suggester-action'],

  classNameBindings: ['isAffirmative:compliance-auto-suggester-action--accept'],

  type: void 0,

  action: noop,

  /**
   * Determines the type of suggestion action this is
   * if type property is passed in
   */
  isAffirmative: computed.equal('type', 'accept'),

  /**
   * Action handler for click event, invokes closure action with type as argument
   */
  click() {
    const { type: intent, action } = getProperties(this, 'type', 'action');

    if (typeof action === 'function') {
      return action(intent);
    }
  }
});
