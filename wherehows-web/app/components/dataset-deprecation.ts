import Component from '@ember/component';
import { inject } from '@ember/service';
import { getProperties, computed, set } from '@ember/object';
import ComputedProperty, { oneWay } from '@ember/object/computed';
import { baseCommentEditorOptions } from 'wherehows-web/constants';
import Notifications, { NotificationEvent } from 'wherehows-web/services/notifications';

export default class DatasetDeprecation extends Component {
  tagName = 'div';

  classNames = ['dataset-deprecation-toggle'];

  /**
   * References the application notifications service
   * @memberof DatasetDeprecation
   */
  notifications = <ComputedProperty<Notifications>>inject();

  /**
   * Flag indicating that the dataset is deprecated or otherwise
   * @type {(null | boolean)}
   * @memberof DatasetDeprecation
   */
  deprecated: null | boolean;

  /**
   * Working reference to the dataset's deprecated flag
   * @memberof DatasetDeprecation
   * @type {ComputedProperty<typeof DatasetDeprecation.deprecated>}
   */
  deprecatedAlias = oneWay('deprecated');

  /**
   * Note accompanying the deprecation flag change
   * @type {string}
   * @memberof DatasetDeprecation
   */
  deprecationNote: string;

  /**
   * Working reference to the dataset's deprecationNote
   * @memberof DatasetDeprecation
   * @type {ComputedProperty<typeof DatasetDeprecation.deprecationNote>}
   */
  deprecationNoteAlias = oneWay('deprecationNote');

  /**
   * Checks the working / aliased copies of the deprecation properties diverge from the
   * saved versions i.e. deprecationNoteAlias and deprecationAlias
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetDeprecation
   */
  isDirty: ComputedProperty<boolean> = computed(
    'deprecatedAlias',
    'deprecated',
    'deprecationNote',
    'deprecationNoteAlias',
    function(this: DatasetDeprecation) {
      const { deprecatedAlias, deprecated, deprecationNote, deprecationNoteAlias } = getProperties(this, [
        'deprecatedAlias',
        'deprecated',
        'deprecationNote',
        'deprecationNoteAlias'
      ]);

      return deprecatedAlias !== deprecated || deprecationNoteAlias !== deprecationNote;
    }
  );

  /**
   * The action to be completed when a save is initiated
   * @type {Function}
   * @memberof DatasetDeprecation
   */
  onUpdateDeprecation: Function;

  editorOptions = {
    ...baseCommentEditorOptions,
    placeholder: {
      text: "You may provide a note about this dataset's deprecation status"
    }
  };

  actions = {
    /**
     * Toggles the boolean value of deprecatedAlias
     */
    toggleDeprecatedStatus(this: DatasetDeprecation) {
      this.toggleProperty('deprecatedAlias');
    },

    /**
     * Invokes the save action with the updated values for 
     * deprecated and deprecationNote
     */
    async onSave(this: DatasetDeprecation) {
      const { deprecatedAlias, deprecationNoteAlias, notifications: { notify } } = getProperties(this, [
        'deprecatedAlias',
        'deprecationNoteAlias',
        'notifications'
      ]);
      const { onUpdateDeprecation } = this;

      if (onUpdateDeprecation) {
        const noteValue = deprecatedAlias ? deprecationNoteAlias : '';

        try {
          await onUpdateDeprecation(deprecatedAlias, noteValue);
          set(this, 'deprecationNoteAlias', noteValue);

          notify(NotificationEvent.success, {
            content: 'Successfully updated deprecation status'
          });
        } catch (e) {
          notify(NotificationEvent.error, {
            content: `An error occurred: ${e.message}`
          });
        }
      }
    }
  };
}
