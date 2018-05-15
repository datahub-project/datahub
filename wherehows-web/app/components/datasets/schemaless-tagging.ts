import Component from '@ember/component';
import { get, computed } from '@ember/object';
import { action } from '@ember-decorators/object';
import { ISecurityClassificationOption } from 'wherehows-web/typings/app/dataset-compliance';
import { getSecurityClassificationDropDownOptions } from 'wherehows-web/constants';

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
   * Flag indicating that the dataset contains personally identifiable data
   * @type {boolean}
   * @memberof SchemalessTagging
   */
  containsPersonalData: boolean;

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
   * Flag indicating if this component should be in edit mode or readonly
   * @type {boolean}
   * @memberof SchemalessTagging
   */
  isEditable: boolean;

  /**
   * The current dataset classification value
   * @type { ISecurityClassificationOption.value}
   * @memberof SchemalessTagging
   */
  classification: ISecurityClassificationOption['value'];

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
