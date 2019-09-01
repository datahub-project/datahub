import Component from '@ember/component';
import { set } from '@ember/object';
import { baseCommentEditorOptions } from 'wherehows-web/constants';
import { action, computed } from '@ember/object';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { classNames } from '@ember-decorators/component';
import { reads } from '@ember/object/computed';

@classNames('dataset-deprecation-toggle')
export default class DatasetDeprecation extends Component {
  /**
   * Currently selected date
   * @type {Date}
   * @memberof DatasetAclAccess
   */
  selectedDate: Date = new Date();

  /**
   * Date around which the calendar is centered
   * @type {Date}
   * @memberof DatasetAclAccess
   */
  centeredDate: Date = this.selectedDate;

  /**
   * Date the dataset should be decommissioned
   * @type {IDatasetView.decommissionTime}
   * @memberof DatasetAclAccess
   */
  decommissionTime: IDatasetView['decommissionTime'];

  /**
   * The earliest date a user can select as a decommission date
   * @type {Date}
   * @memberof DatasetAclAccess
   */
  minSelectableDecommissionDate: Date = new Date(Date.now() + 24 * 60 * 60 * 1000);

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
  @reads('deprecated')
  deprecatedAlias: boolean;

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
  @reads('deprecationNote')
  deprecationNoteAlias: string;

  /**
   * Before a user can update the deprecation status to deprecated, they must acknowledge that even if the
   * dataset is deprecated they must still keep it compliant.
   * @memberof DatasetDeprecation
   * @type {boolean}
   */
  isDeprecationNoticeAcknowledged: boolean = false;

  /**
   * Checks the working / aliased copies of the deprecation properties diverge from the
   * saved versions i.e. deprecationNoteAlias and deprecationAlias
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetDeprecation
   */
  @computed('deprecatedAlias', 'deprecated', 'deprecationNote', 'deprecationNoteAlias')
  get isDirty(): boolean {
    const { deprecatedAlias, deprecated, deprecationNote, deprecationNoteAlias } = this;

    return deprecatedAlias !== deprecated || deprecationNoteAlias !== deprecationNote;
  }

  /**
   * The external action to be completed when a save is initiated
   * @type {(isDeprecated: boolean, updateDeprecationNode: string, decommissionTime: Date | null) => Promise<void>}
   * @memberof DatasetDeprecation
   */
  onUpdateDeprecation: (
    isDeprecated: boolean,
    updateDeprecationNode: string,
    decommissionTime: Date | null
  ) => Promise<void> | void;

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
   * Handles updates to the decommissionTime attribute
   * @param {Date} decommissionTime date dataset should be decommissioned
   */
  @action
  onDecommissionDateChange(this: DatasetDeprecation, decommissionTime: Date) {
    set(this, 'decommissionTime', new Date(decommissionTime).getTime());
  }

  /**
   * When a user clicks the checkbox to acknowledge or cancel acknowledgement of the notice for
   * deprecating a dataset
   */
  @action
  onAcknowledgeDeprecationNotice(this: DatasetDeprecation) {
    this.toggleProperty('isDeprecationNoticeAcknowledged');
  }

  /**
   * Invokes the save action with the updated values for
   * deprecated decommissionTime, and deprecationNote
   * @return {Promise<void>}
   */
  @action
  async onSave(this: DatasetDeprecation) {
    const { deprecatedAlias, deprecationNoteAlias, decommissionTime } = this;
    const { onUpdateDeprecation } = this;

    if (onUpdateDeprecation) {
      const noteValue = deprecatedAlias ? deprecationNoteAlias : '';
      const time = decommissionTime ? new Date(decommissionTime) : null;

      await onUpdateDeprecation(!!deprecatedAlias, noteValue || '', time);
      set(this, 'deprecationNoteAlias', noteValue);
    }
  }
}
