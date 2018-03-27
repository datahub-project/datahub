import Component from '@ember/component';
import ComputedProperty from '@ember/object/computed';
import { computed, getProperties } from '@ember/object';
import { IComplianceChangeSet } from 'wherehows-web/components/dataset-compliance';
import noop from 'wherehows-web/utils/noop';
import { SuggestionIntent } from 'wherehows-web/constants';

/**
 * Describes the interface for the auto-suggest-action component
 * @interface IAutoSuggestAction
 */
interface IAutoSuggestAction {
  type: SuggestionIntent;
  action: Function;
  isAffirmative: ComputedProperty<boolean>;
  field: IComplianceChangeSet;
  feedbackAction: (uid: string | null, feedback: SuggestionIntent) => void;
}

export default Component.extend(<IAutoSuggestAction>{
  tagName: 'button',

  classNames: ['compliance-auto-suggester-action'],

  classNameBindings: ['isAffirmative:compliance-auto-suggester-action--accept'],

  type: SuggestionIntent.ignore,

  action: noop,

  feedbackAction: noop,

  field: <IComplianceChangeSet>{},

  /**
   * Determines the type of suggestion action this is
   * if type property is passed in
   */
  isAffirmative: computed.equal('type', 'accept'),

  /**
   * Action handler for click event, invokes closure action with type as argument
   */
  click() {
    const { type: intent, action, field, feedbackAction } = getProperties(
      this,
      'type',
      'action',
      'field',
      'feedbackAction'
    );
    const { uid } = field.suggestion!;

    if (typeof action === 'function') {
      feedbackAction(uid || null, intent);
      return action(intent);
    }
  }
});
