import Component from '@ember/component';
import { get } from '@ember/object';
import { ISecurityClassificationOption, securityClassificationDropdownOptions } from 'wherehows-web/constants';

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
  classifiers: Array<ISecurityClassificationOption> = securityClassificationDropdownOptions;

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

  actions = {
    /**
     * Invokes the closure action onPersonaDataChange when the flag is toggled
     * @param {boolean} containsPersonalDataTag flag indicating that the dataset contains personal data
     * @returns boolean
     */
    onPersonalDataToggle(this: SchemalessTagging, containsPersonalDataTag: boolean) {
      return get(this, 'onPersonalDataChange')(containsPersonalDataTag);
    },

    /**
     * Updates the dataset security classification via the closure action onClassificationChange
     * @param {ISecurityClassificationOption} { value } security Classification value for the dataset
     * @returns null | Classification
     */
    onSecurityClassificationChange(this: SchemalessTagging, { value }: ISecurityClassificationOption) {
      const securityClassification = value || null;
      return get(this, 'onClassificationChange')(securityClassification);
    }
  };
}
