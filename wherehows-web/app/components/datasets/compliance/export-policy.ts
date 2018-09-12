import Component from '@ember/component';
import { ComplianceEdit, ExportPolicyKeys } from 'wherehows-web/constants';
import { noop } from 'wherehows-web/utils/helpers/functions';
import { IDatasetExportPolicy } from 'wherehows-web/typings/api/datasets/compliance';
import { IExportPolicyTable } from 'wherehows-web/typings/app/datasets/export-policy';
import { get, set } from '@ember/object';
import { action, computed } from '@ember-decorators/object';
import { or } from '@ember-decorators/object/computed';
import ComputedProperty from '@ember/object/computed';

enum ExportPolicyLabels {
  UGC = 'User Generated Content - data directly created by the member',
  UAGC = 'User Action Generated Content - data created as a result of direct member action on the site',
  UDC = 'User Derived Content - data owned by the member but not directly created by the member or due to member actions on the site'
}

/**
 * Creates an array from the ExportPolicyKeys enum
 */
const policyKeys = <Array<keyof typeof ExportPolicyKeys>>Object.keys(ExportPolicyKeys);
// Values in the same order as our keys
const policyValues = <Array<ExportPolicyKeys>>policyKeys.map(key => ExportPolicyKeys[key]);

export default class ComplianceExportPolicy extends Component.extend({
  /**
   * Sets the tag for the html element rendered by our component
   * @type {string}
   */
  tagName: ''
}) {
  /**
   * Allows us to use our constants set in this enum inside our hbs template
   * @type {ComplianceEdit}
   */
  ComplianceEdit = ComplianceEdit;

  /**
   * Passed in from the parent component, this determines the edit state of the compliance tab
   * @type {boolean}
   */
  isEditing: boolean;

  /**
   * Passed through ation from the parent component, this function acts as an action to trigger edit mode
   * on this component
   * @type {(edit: boolean, target: ComplianceEdit) => void}
   */
  toggleEditing: (edit: boolean, target?: ComplianceEdit) => void;

  /**
   * Passed through action from the parent component, this function triggers the saving logic and subsequent
   * POST request to persist the export policy edits
   * @type {(policy: IDatasetExportPolicy) => void}
   */
  onSaveExportPolicy: (policy: IDatasetExportPolicy) => void;

  /**
   * User triggered flag in which they desire to see all rows of the export policy by clicking "show more"
   * @type {boolean}
   * @memberof ComplianceExportPolicy
   */
  shouldShowMorePolicyData = false;

  /**
   * Whether we should show all of the export policy rows. Based on whether we are editing or user desires
   * to see all the rows
   * @type {boolean}
   * @memberof ComplianceExportPolicy
   */
  @or('isEditing', 'shouldShowMorePolicyData')
  shouldShowAllExportPolicyData: ComputedProperty<boolean>;

  /**
   * The export policy data extracted directly from the api response, passed in from the dataset-compliance
   * container and stored here as its original response
   * @type {IDatasetExportPolicy}
   * @memberof ComplianceExportPolicy
   */
  exportPolicyData: IDatasetExportPolicy | undefined;

  @computed('exportPolicyData', 'isEditing')
  get showExportPolicyUndefined(): boolean {
    return !get(this, 'isEditing') && !get(this, 'exportPolicyData');
  }

  /**
   * Calculates the table for the export policy questions with seeded information based on the api response
   * for the dataset's export policy
   * @type {ComputedProperty<Array<IExportPolicyTable>>}
   * @memberof ComplianceExportPolicy
   */
  @computed('exportPolicyData', 'isEditing')
  get datasetExportPolicy(): Array<IExportPolicyTable> {
    const exportPolicyData = <IDatasetExportPolicy>(get(this, 'exportPolicyData') || {});

    return policyKeys.map(key => {
      const dataType = ExportPolicyKeys[key];

      return {
        dataType,
        value: exportPolicyData[dataType] || false,
        label: ExportPolicyLabels[key]
      };
    });
  }

  constructor() {
    super(...arguments);
    // Setting our default values for properties expected from parent
    typeof this.isEditing === 'boolean' || (this.isEditing = false);
    this.toggleEditing || (this.toggleEditing = noop);
    this.onSaveExportPolicy || (this.onSaveExportPolicy = noop);
  }

  /**
   * Action to mark all fields in our working copy as false for the export policy
   */
  @action
  onMarkAllExportFieldsAsFalse(this: ComplianceExportPolicy): void {
    const exportPolicy = get(this, 'datasetExportPolicy');

    exportPolicy.forEach(policy => {
      set(policy, 'value', false);
    });
  }

  /**
   * Action triggered by the user clicking on the button "Show More" or "Show Less" that toggles
   * the property that controls how much of the table we are showing
   */
  @action
  onShowAllExportPolicyData(this: ComplianceExportPolicy): void {
    this.toggleProperty('shouldShowMorePolicyData');
  }

  /**
   * Action triggered by a user edit when they click on the "Yes" or "No" radio buttons in edit
   * mode. This allows us to record and show changes that user has made in our working copy.
   * @param {ExportPolicyKeys} dataType - the type of DMA annotation for export policy
   * @param {boolean} value - whether or not the dataset contains this type of data
   */
  @action
  onChangeExportPolicy(dataType: ExportPolicyKeys, value: boolean): void {
    set(get(this, 'datasetExportPolicy')[policyValues.indexOf(dataType)], 'value', value);
  }

  /**
   * Save action that should ultimately call the POST request to update our changes to the export
   * policy information. First, we build our POST request from the working copy
   */
  @action
  saveCompliance(): void {
    const datasetExportPolicy = get(this, 'datasetExportPolicy');
    const exportPolicy = datasetExportPolicy.reduce((policy, datum) => ({ ...policy, [datum.dataType]: datum.value }), <
      IDatasetExportPolicy
    >{});

    this.onSaveExportPolicy(exportPolicy);
  }

  /**
   * Action that helps us cancel edit mode in the parent component by calling the toggleEditing
   * action that was passed down to us. Note, onCancel connects to the generic partial, which is why
   * we can't just call toggleEditing directly in the template
   */
  @action
  onCancel(): void {
    this.toggleEditing(false);
  }
}
