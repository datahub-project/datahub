import Component from '@ember/component';
import { computed, action } from '@ember/object';
import { ISecurityClassificationOption } from 'datahub-web/typings/app/dataset-compliance';
import { getSecurityClassificationDropDownOptions } from 'datahub-web/constants';
import { noop } from 'lodash';
import { classNames } from '@ember-decorators/component';
import { Classification } from '@datahub/metadata-types/constants/entity/dataset/compliance-field-types';

// TODO: [META-12152] This module should be migrated out of data-portal along with the dependent
// constants file

/**
 * Defines the edit states that the compliance policy component can have. Each one corresponds to a
 * different section/component being edited on the compliance tab
 */
enum ComplianceEdit {
  CompliancePolicy = 'editCompliancePolicy',
  PurgePolicy = 'editPurgePolicy',
  DatasetLevelPolicy = 'editDatasetLevelCompliancePolicy',
  ExportPolicy = 'editExportPolicy'
}

@classNames('schemaless-tagging')
export default class SchemalessTagging extends Component {
  /**
   * Interface for parent supplied onPersonalDataChange action
   * @memberof SchemalessTagging
   */
  onPersonalDataChange: (containsPersonalData: boolean) => boolean;

  /**
   * Interface for parent supplied onClassificationChange action
   * @memberof SchemalessTagging
   */
  onClassificationChange: (
    securityClassification: ISecurityClassificationOption['value']
  ) => ISecurityClassificationOption['value'];

  /**
   * Used to pass around constants in the component or template
   */
  ComplianceEdit = ComplianceEdit;

  /**
   * Flag indicating that the dataset contains personally identifiable data
   * @type {boolean}
   * @memberof SchemalessTagging
   */
  containsPersonalData: boolean;

  /**
   * Passed through flag that determines whether or not we are in an editing state for this
   * component
   * @type {boolean}
   */
  isEditing = false;

  /**
   * List of drop down options for classifying the dataset
   * @type {Array<ISecurityClassificationOption>}
   * @memberof SchemalessTagging
   */
  @computed('containsPersonalData')
  get classifiers(): Array<ISecurityClassificationOption> {
    return getSecurityClassificationDropDownOptions(this.containsPersonalData);
  }

  /**
   * The current dataset classification value
   * @type {ISecurityClassificationOption.value}
   * @memberof SchemalessTagging
   */
  classification: ISecurityClassificationOption['value'];

  /**
   * Passed through action from the parent dataset-compliance component that toggles editing
   * at the container level
   * @type {(e: boolean) => void}
   */
  toggleEditing: (edit: boolean) => void = noop;

  /**
   * Passed through action from the parent dataset-compliance component that triggers a save
   * action for the compliance information
   */
  onSaveCompliance: () => Promise<void> = () => Promise.resolve();

  /**
   * Passed through action from the parent dataset-compliance component that triggers a reset
   * for our compliance information, effectively rolling us back to the server state
   * @type {() => void}
   */
  resetCompliance: () => void = noop;

  /**
   * Action in the template action bar that the user clicks to save their changes to the dataset
   * level classification
   */
  @action
  async saveCompliance(): Promise<void> {
    await this.onSaveCompliance();
    this.toggleEditing(false);
  }

  /**
   * Action in the template action bar that the user clicks to cancel their changes. It sets our
   * editing flag to false and re-fetches information from the server to "roll back" our changes
   */
  @action
  onCancel(): void {
    this.toggleEditing(false);
    this.resetCompliance();
  }

  /**
   * Invokes the closure action onPersonaDataChange when the flag is toggled
   * @param {boolean} containsPersonalDataTag flag indicating that the dataset contains personal data
   * @returns boolean
   */
  @action
  onPersonalDataToggle(containsPersonalDataTag: boolean): boolean {
    if (containsPersonalDataTag) {
      this.actions.onSecurityClassificationChange.call(this, { value: null });
    }

    return this.onPersonalDataChange(containsPersonalDataTag);
  }

  /**
   * Updates the dataset security classification via the closure action onClassificationChange
   * @param {ISecurityClassificationOption} { value } security Classification value for the dataset
   * @returns null | Classification
   */
  @action
  onSecurityClassificationChange({ value }: { value: ISecurityClassificationOption['value'] }): null | Classification {
    const securityClassification = value || null;
    return this.onClassificationChange(securityClassification);
  }
}
