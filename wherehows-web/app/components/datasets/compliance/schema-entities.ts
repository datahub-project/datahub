import Component from '@ember/component';
import { action } from '@ember-decorators/object';
import { set } from '@ember/object';
import { noop } from 'wherehows-web/utils/helpers/functions';
import { ComplianceEdit, TagFilter, ComplianceFieldIdValue } from 'wherehows-web/constants';
import { htmlSafe } from '@ember/string';
import { IComplianceChangeSet } from 'wherehows-web/typings/app/dataset-compliance';
import { TrackableEventCategory, trackableEvent } from 'wherehows-web/constants/analytics/event-tracking';

/**
 * The ComplianceSchemaEntities component allows the user to individually tag fields to annotate the
 * data contained in the dataset schema, found in the Compliance tab for datasets.
 */
export default class ComplianceSchemaEntities extends Component.extend({}) {
  /**
   * Passed in flag determining whether or not we are in an editing mode for the schema entities.
   * @type {boolean}
   */
  isEditing: boolean;

  /**
   * Passed in computed prop, Lists the IComplianceChangeSet / tags without an identifierType value
   * @type {Array<IComplianceChangeSet>}
   */
  unspecifiedTags: Array<IComplianceChangeSet>;

  /**
   * Passed in action/method from parent, updates the editing mode and corresponding target
   */
  toggleEditing: (e: boolean, target?: ComplianceEdit) => void;

  /**
   * Passed in action from parent, updates showGuidedEditMode state on parent
   */
  // Note: [REFACTOR-COMPLIANCE] This passed in function is to maintain status quo while we migrate the
  // refactor logic and should eventually no longer be required
  showGuidedEditMode: (b: boolean) => void;

  /**
   * Passed in action from parent, updates fieldReviewOption state on parent
   */
  // Note: [REFACTOR-COMPLIANCE] Same as above showGuidedEditMode
  fieldReviewChange: (o: { value: TagFilter }) => TagFilter;

  /**
   * Specifies the filter to be applied on the list of fields shown in the compliance policy table
   * @type {TagFilter}
   * @memberof DatasetCompliance
   */
  // Note: [REFACTOR-COMPLIANCE] This value will currently be passed in from the parent but should
  // eventually live only on this component
  fieldReviewOption!: TagFilter;

  /**
   * Used in the template to help pass values for the edit target
   * @type {ComplianceEdit}
   */
  ComplianceEdit = ComplianceEdit;

  /**
   * References the ComplianceFieldIdValue enum, which specifies hardcoded values for field
   * identifiers
   * @type {ComplianceFieldIdValue}
   */
  ComplianceFieldIdValue = ComplianceFieldIdValue;

  /**
   * Enum of categories that can be tracked for this component
   * @type {TrackableEventCategory}
   */
  trackableCategory = TrackableEventCategory;

  /**
   * Map of events that can be tracked
   * @type {ITrackableEventCategoryEvent}
   */
  trackableEvent = trackableEvent;

  /**
   * Flag indicating the current compliance policy edit-view mode. Guided edit mode allows users
   * to go through a wizard to edit the schema entities while the other method is direct JSON editing
   * @type {boolean}
   * @memberof ComplianceSchemaEntities
   */
  showGuidedComplianceEditMode = true;

  /**
   * A list of ui values and labels for review filter drop-down
   * @type {Array<{value: TagFilter, label:string}>}
   * @memberof ComplianceSchemaEntities
   */
  fieldReviewOptions: Array<{ value: ComplianceSchemaEntities['fieldReviewOption']; label: string }> = [
    { value: TagFilter.showAll, label: '  Show all fields' },
    { value: TagFilter.showReview, label: '? Show fields missing a data type' },
    { value: TagFilter.showSuggested, label: '! Show fields that need review' },
    //@ts-ignore htmlSafe type definition is incorrect in @types/ember contains TODO: to return Handlebars.SafeStringStatic
    { value: TagFilter.showCompleted, label: <string>htmlSafe(`&#10003; Show completed fields`) }
  ];

  constructor() {
    super(...arguments);

    // Default values for undefined properties
    this.unspecifiedTags || (this.unspecifiedTags = []);
    this.toggleEditing || (this.toggleEditing = noop);
    this.showGuidedEditMode || (this.showGuidedEditMode = noop);
    this.fieldReviewChange || (this.fieldReviewChange = noop);
  }

  /**
   * Toggle the visibility of the guided compliance edit view vs the advanced (json) edit view modes
   * @param {boolean} toggle flag ,if true, show guided edit mode, otherwise, advanced
   */
  @action
  onShowGuidedEditMode(this: ComplianceSchemaEntities, toggle: boolean): void {
    const isShowingGuidedEditMode = set(this, 'showGuidedComplianceEditMode', toggle);
    // Note: [REFACTOR-COMPLIANCE] Should be deleted once full functionality lives on this component
    this.showGuidedEditMode(isShowingGuidedEditMode);
  }

  /**
   * Invokes the external action to update the tagFilter query
   * @param {{value: TagFilter}} { value }
   * @returns {TagFilter}
   */
  @action
  onFieldReviewChange(this: ComplianceSchemaEntities, option: { value: TagFilter }): TagFilter {
    // Note: [REFACTOR-COMPLIANCE] The passed in effects should eventually live only on this component
    return this.fieldReviewChange(option);
  }
}
