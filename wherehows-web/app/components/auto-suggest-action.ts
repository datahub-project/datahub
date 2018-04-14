import Component from '@ember/component';
import ComputedProperty from '@ember/object/computed';
import { computed, getProperties } from '@ember/object';
import { SuggestionIntent } from 'wherehows-web/constants';
import { IComplianceChangeSet } from 'wherehows-web/typings/app/dataset-compliance';

export default class AutoSuggestAction extends Component {
  tagName = 'button';

  classNames = ['compliance-auto-suggester-action'];

  classNameBindings = ['isAffirmative:compliance-auto-suggester-action--accept'];

  /**
   * Value of the user's action / reaction to suggested value
   * @type {SuggestionIntent}
   * @memberof AutoSuggestAction
   */
  type: SuggestionIntent;

  /**
   * Describes the interface for the external action `onSuggestionClick`
   * @memberof AutoSuggestAction
   */
  onSuggestionClick: (intent: SuggestionIntent) => void;

  /**
   * Describes the interface for the external action `feedbackAction`
   * @memberof AutoSuggestAction
   */
  feedbackAction: (uid: string | null, intent: SuggestionIntent) => void;

  /**
   * References the tag / field for which this suggestion should apply
   * @type {IComplianceChangeSet}
   * @memberof AutoSuggestAction
   */
  field: IComplianceChangeSet;

  constructor() {
    super(...arguments);

    this.type || (this.type = SuggestionIntent.ignore);
  }

  /**
   * Determines the type of suggestion action this is
   * if type property is passed in
   */
  isAffirmative: ComputedProperty<boolean> = computed.equal('type', 'accept');

  /**
   * Action handler for click event, invokes closure action with type as argument
   */
  click(): void {
    const { type: intent, onSuggestionClick, field, feedbackAction } = getProperties(
      this,
      'type',
      'onSuggestionClick',
      'field',
      'feedbackAction'
    );
    const { uid } = field.suggestion!;

    if (typeof onSuggestionClick === 'function') {
      feedbackAction(uid || null, intent);
      return onSuggestionClick(intent);
    }
  }
}
