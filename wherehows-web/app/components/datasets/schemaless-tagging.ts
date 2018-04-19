import Component from '@ember/component';
import { get } from '@ember/object';
import { action } from 'ember-decorators/object';

export default class SchemalessTagging extends Component {
  classNames = ['schemaless-tagging'];

  /**
   * Interface for parent supplied onPersonalDataChange action
   * @memberof SchemalessTagging
   */
  onPersonalDataChange: (containsPersonalData: boolean) => boolean;

  /**
   * Flag indicating that the dataset contains personally identifiable data
   * @type {boolean}
   * @memberof SchemalessTagging
   */
  containsPersonalData: boolean;

  /**
   * Flag indicating if this component should be in edit mode or readonly
   * @type {boolean}
   * @memberof SchemalessTagging
   */
  isEditable: boolean;

  /**
   * Invokes the closure action onPersonaDataChange when the flag is toggled
   * @param {boolean} containsPersonalDataTag flag indicating that the dataset contains personal data
   * @returns boolean
   */
  @action
  onPersonalDataToggle(this: SchemalessTagging, containsPersonalDataTag: boolean) {
    return get(this, 'onPersonalDataChange')(containsPersonalDataTag);
  }
}
