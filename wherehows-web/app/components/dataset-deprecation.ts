import Component from '@ember/component';
import { getProperties, computed, set } from '@ember/object';
import ComputedProperty, { oneWay } from '@ember/object/computed';
import { baseCommentEditorOptions } from 'wherehows-web/constants';
import { action } from 'ember-decorators/object';

export default class DatasetDeprecation extends Component {
  tagName = 'div';

  classNames = ['dataset-deprecation-toggle'];

  /**
   * Flag indicating that the dataset is deprecated or otherwise
   * @type {(null | boolean)}
   * @memberof DatasetDeprecation
   */
  deprecated: null | boolean;

  /**
   * Working reference to the dataset's deprecated flag
   * @memberof DatasetDeprecation
   * @type {ComputedProperty<DatasetDeprecation.deprecated>}
   */
  deprecatedAlias: ComputedProperty<DatasetDeprecation['deprecated']> = oneWay('deprecated');

  /**
   * Note accompanying the deprecation flag change
   * @type {string}
   * @memberof DatasetDeprecation
   */
  deprecationNote: string;

  /**
   * Working reference to the dataset's deprecationNote
   * @memberof DatasetDeprecation
   * @type {ComputedProperty<DatasetDeprecation.deprecationNote>}
   */
  deprecationNoteAlias: ComputedProperty<DatasetDeprecation['deprecationNote']> = oneWay('deprecationNote');

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
   * The external action to be completed when a save is initiated
   * @type {(isDeprecated: boolean, updateDeprecationNode: string) => Promise<void>}
   * @memberof DatasetDeprecation
   */
  onUpdateDeprecation: (isDeprecated: boolean, updateDeprecationNode: string) => Promise<void> | void;

  editorOptions = {
    ...baseCommentEditorOptions,
    placeholder: {
      text: "You may provide a note about this dataset's deprecation status"
    }
  };

  /**
   * Toggles the boolean value of deprecatedAlias
   */
  @action
  toggleDeprecatedStatus(this: DatasetDeprecation) {
    this.toggleProperty('deprecatedAlias');
  }

  /**
   * Invokes the save action with the updated values for
   * deprecated and deprecationNote
   * @return {Promise<void>}
   */
  @action
  async onSave(this: DatasetDeprecation) {
    const { deprecatedAlias, deprecationNoteAlias } = getProperties(this, ['deprecatedAlias', 'deprecationNoteAlias']);
    const { onUpdateDeprecation } = this;

    if (onUpdateDeprecation) {
      const noteValue = deprecatedAlias ? deprecationNoteAlias : '';

      await onUpdateDeprecation(!!deprecatedAlias, noteValue || '');
      set(this, 'deprecationNoteAlias', noteValue);
    }
  }
}
