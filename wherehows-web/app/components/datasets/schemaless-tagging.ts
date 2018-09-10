import Component from '@ember/component';
import { get, computed } from '@ember/object';
import { action } from '@ember-decorators/object';
import { ISecurityClassificationOption } from 'wherehows-web/typings/app/dataset-compliance';
import { getSecurityClassificationDropDownOptions, ComplianceEdit } from 'wherehows-web/constants';
import { noop } from 'wherehows-web/utils/helpers/functions';

export default class SchemalessTagging extends Component {
  classNames = ['schemaless-tagging'];

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
  isEditing: boolean;

  /**
   * List of drop down options for classifying the dataset
   * @type {Array<ISecurityClassificationOption>}
   * @memberof SchemalessTagging
   */
  classifiers = computed('containsPersonalData', function(
    this: SchemalessTagging
  ): Array<ISecurityClassificationOption> {
    return getSecurityClassificationDropDownOptions(get(this, 'containsPersonalData'));
  });

  /**
   * The current dataset classification value
   * @type { ISecurityClassificationOption.value}
   * @memberof SchemalessTagging
   */
  classification: ISecurityClassificationOption['value'];

  /**
   * Passed through action from the parent dataset-compliance component that toggles editing
   * at the container level
   * @type {(e: boolean) => void}
   */
  toggleEditing: (edit: boolean) => void;

  /**
   * Passed through action from the parent dataset-compliance component that triggers a save
   * action for the compliance information
   * @type {() => Promise<void>}
   */
  onSaveCompliance: () => Promise<void>;

  /**
   * Passed through action from the parent dataset-compliance component that triggers a reset
   * for our compliance information, effectively rolling us back to the server state
   * @type {() => void}
   */
  resetCompliance: () => void;

  constructor() {
    super(...arguments);

    typeof this.isEditing === 'boolean' || (this.isEditing = false);
    this.toggleEditing || (this.toggleEditing = noop);
    this.onSaveCompliance || (this.onSaveCompliance = noop);
    this.resetCompliance || (this.resetCompliance = noop);
  }

  /**
   * Action in the template action bar that the user clicks to save their changes to the dataset
   * level classification
   */
  @action
  saveCompliance() {
    this.onSaveCompliance();
  }

  /**
   * Action in the template action bar that the user clicks to cancel their changes. It sets our
   * editing flag to false and re-fetches information from the server to "roll back" our changes
   */
  @action
  onCancel() {
    this.toggleEditing(false);
    this.resetCompliance();
  }

  /**
   * Invokes the closure action onPersonaDataChange when the flag is toggled
   * @param {boolean} containsPersonalDataTag flag indicating that the dataset contains personal data
   * @returns boolean
   */
  @action
  onPersonalDataToggle(this: SchemalessTagging, containsPersonalDataTag: boolean) {
    if (containsPersonalDataTag) {
      this.actions.onSecurityClassificationChange.call(this, { value: null });
    }

    return get(this, 'onPersonalDataChange')(containsPersonalDataTag);
  }

  /**
   * Updates the dataset security classification via the closure action onClassificationChange
   * @param {ISecurityClassificationOption} { value } security Classification value for the dataset
   * @returns null | Classification
   */
  @action
  onSecurityClassificationChange(
    this: SchemalessTagging,
    { value }: { value: ISecurityClassificationOption['value'] }
  ) {
    const securityClassification = value || null;
    return get(this, 'onClassificationChange')(securityClassification);
  }
}
