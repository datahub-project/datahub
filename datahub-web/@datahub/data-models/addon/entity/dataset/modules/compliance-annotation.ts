import { IComplianceFieldAnnotation } from '@datahub/metadata-types/constants/entity/dataset/compliance-field-annotation';
import { annotationIsValid } from '@datahub/data-models/entity/dataset/modules/constants/annotation-helpers';
import { set, setProperties, computed } from '@ember/object';
import { ComplianceAnnotationsEditableProps } from '@datahub/data-models/entity/dataset/helpers/validators/compliance/annotations';
import { IComplianceDataType } from '@datahub/metadata-types/types/entity/dataset/compliance-data-types';
import { pick, omit, isEqual } from 'lodash';
/**
 * Creates a basic factory to generate default values for a compliance annotation tag. This class is created both to
 * translate the API response for annotations as well as when the user initiates the creation of a new tag from the UI. In
 * the latter case, the process is a step by step user flow, so we start with these undefined fields and then start to
 * fill them out throughout the process.
 */
const complianceAnnotationFactory = (fieldName: string): IComplianceFieldAnnotation => ({
  identifierField: fieldName,
  identifierType: undefined,
  logicalType: null,
  nonOwner: undefined,
  pii: false,
  readonly: false,
  securityClassification: null,
  valuePattern: undefined
});

/**
 * Annotation fields that can be modified on the DatasetComplianceAnnotation class while a user is actively annotating a field
 * Extracted out of class to allow ComputedProperty dependent key reference
 */
const modifiableKeysOnClassData: Array<ComplianceAnnotationsEditableProps> = [
  'identifierField',
  'identifierType',
  'logicalType',
  'isPurgeKey',
  'valuePattern',
  'isReadOnly'
];

/**
 * A dataset is expected to have a number of fields that can be tagged with metadata regarding what kind of
 * information that dataset field contains (namely in terms of PII). Each field has an identifierField to
 * tell us the field name, and a number of tags (annotations) tied to that identifierField. This class is the
 * objectified representation of a single instance of such a tag.
 */
export default class DatasetComplianceAnnotation implements IComplianceFieldAnnotation {
  /**
   * The original data that initialized this class. Stored here and expected to remain static. Allows us to
   * maintain the original data read from the API. The initialization of this class is not connected to the
   * API for each individual tag, so this property does not need an async getter
   * @type {IComplianceFieldAnnotation}
   */
  private readonly data: Readonly<IComplianceFieldAnnotation>;

  static modifiableKeysOnRawData(): Array<string> {
    return ['identifierField', 'identifierType', 'logicalType', 'nonOwner', 'valuePattern', 'readonly'];
  }

  /**
   * Static class reference to the modifiableKeysOnClassData list.
   * @static
   */
  static modifiableKeysOnClassData(): Array<ComplianceAnnotationsEditableProps> {
    return modifiableKeysOnClassData;
  }

  /**
   * Whether or not there have been changes made to this annotation tag from the base data that created it.
   */
  isDirty = false;

  /**
   * The field name that this annotation tag is tied to. Class cannot be initialized with this undefined
   */
  identifierField!: IComplianceFieldAnnotation['identifierField'];

  /**
   * Determines which kind of tag we are dealing with. If a newly created tag, this item starts undefined
   * and becomes defined if the person chooses a tag for the field in the specified dataset.
   */
  identifierType?: IComplianceFieldAnnotation['identifierType'];

  /**
   * Tag indicating the type of the identifierType,
   * solely applicable to corresponding id's with an idType of `false` in the complianceDataTypes endpoint
   */
  logicalType?: IComplianceFieldAnnotation['logicalType'];

  /**
   * Whether the tag is specified as a "purge key" in the UI by the user in their annotation. Null is allowable
   * only as a working copy construct as sometimes it is necessary to specifically assign null instead of
   * undefined for no-value situations
   */
  isPurgeKey?: boolean | null;

  /**
   * Whether the field is flagged as containing PII data
   */
  pii!: IComplianceFieldAnnotation['pii'];

  /**
   * User specified / default security classification for the related schema field. Null is allowable only
   * as a working copy construct as it is sometimes necessary to specifically assign null instead of undefined
   * for no-value situations
   */

  securityClassification!: IComplianceFieldAnnotation['securityClassification'];

  /**
   * Optional attribute for the value of a CUSTOM regex. Required for CUSTOM field format
   */
  valuePattern?: IComplianceFieldAnnotation['valuePattern'];

  /**
   * Whether the field should be read only (and therefore requires override to be editable)
   */
  isReadOnly!: IComplianceFieldAnnotation['readonly'];

  /**
   * Flags a divergence between the underlying previously saved IComplianceFieldAnnotation and attributes on the working copy
   * @readonly
   */
  @computed('data', ...modifiableKeysOnClassData)
  get hasChangedFromSavedAnnotation(): boolean {
    const { data } = this;
    const { modifiableKeysOnRawData, modifiableKeysOnClassData } = DatasetComplianceAnnotation;
    const { nonOwner, ...remainingSourceAnnotation } = pick(data, modifiableKeysOnRawData());
    const { isPurgeKey, ...remainingPossibleUpdates } = pick(this, modifiableKeysOnClassData());
    /**
     * Values of this[isReadOnly] / data[readonly] are not considered when comparing annotation values since they are irrelevant to equality
     * isPurgeKey is a *reversal* of nonOwner, they are considered equal / unchanged for all other values except if their boolean values are equal
     */
    const purgeKeyHasChanged =
      typeof nonOwner === 'boolean' && typeof isPurgeKey === 'boolean' ? isPurgeKey === nonOwner : false;
    const attributesHaveChanged =
      !isEqual(omit(remainingSourceAnnotation, 'readonly'), omit(remainingPossibleUpdates, 'isReadOnly')) ||
      purgeKeyHasChanged;

    // identifierType being present on the source data is a trusted indicator that this may have been saved perviously
    return Boolean(data.identifierType && attributesHaveChanged);
  }

  /**
   * Creates or resets the fields of this class by applying the original data over the current items. Allows
   * us to initialize a working copy of the class or revert back if the user has canceled their changes
   */
  createWorkingCopy(): void {
    const data = this.data;
    const keys: Array<keyof IComplianceFieldAnnotation & keyof DatasetComplianceAnnotation> = [
      'identifierField',
      'logicalType',
      'pii',
      'securityClassification'
    ];
    // Assigns each value from the object given in the constructor or factory to the corresponding
    // class key in the definition
    keys.forEach((key): void => {
      set(this, key, typeof data[key] === 'object' ? JSON.parse(JSON.stringify(data[key])) : data[key]);
    });

    // Leftover for non-matching keys
    // Upon initialization or reverting, we assume that the working copy is no longer dirtied with changes
    setProperties(this, {
      identifierType: data.identifierType || null,
      valuePattern: data.valuePattern || null,
      isPurgeKey: typeof data.nonOwner === 'boolean' ? data.nonOwner === false : null,
      // Protection vs undefined case
      isReadOnly: data.readonly || false,
      isDirty: false
    });
  }

  /**
   * Creates an API friendly object from our classified working copy. This way, we can iterate through a list
   * of these annotation objects calling this method to create a batch POST request
   */
  readWorkingCopy(): IComplianceFieldAnnotation {
    const {
      identifierField,
      identifierType,
      logicalType,
      pii,
      securityClassification,
      valuePattern,
      isReadOnly
    } = this;

    return {
      identifierField,
      logicalType,
      pii,
      securityClassification,
      identifierType: identifierType || null,
      valuePattern: valuePattern || null,
      nonOwner: typeof this.isPurgeKey === 'boolean' ? !this.isPurgeKey : null,
      readonly: isReadOnly || false
    };
  }

  /**
   * Exposes a copy of the private underlying compliance annotation
   */
  readOriginalCopy(): IComplianceFieldAnnotation {
    return JSON.parse(JSON.stringify(this.data));
  }

  /**
   * This function calls on createWorkingCopy as both perform the same functionality, but having this makes
   * the interface more consumer friendly.
   */
  resetWorkingCopy(): void {
    this.createWorkingCopy();
  }

  /**
   * Used to determine whether this annotation is currently a valid tag. Ruleset is determined as a definition
   * since in open source the rules for what is valid may vary
   */
  isValidAnnotation(complianceDataTypes?: Array<IComplianceDataType>): boolean {
    return annotationIsValid(this, complianceDataTypes);
  }

  /**
   * Serializes `this` compliance annotation attributes to a match the field compliance interface which is supported by newer
   * annotation / compliance endpoints
   */
  toFieldCompliance(): Com.Linkedin.Dataset.FieldCompliance {
    return DatasetComplianceAnnotation.toFieldCompliance(this);
  }

  /**
   * Serializes a provided compliance annotation instance of field annotation attributes and maps to an object
   * that matches the field compliance interface which is supported by newer
   * annotation / compliance endpoints
   * @static
   * @param {(DatasetComplianceAnnotation | IComplianceFieldAnnotation)} annotation the annotation attributes to transform, can either be an instance of this class
   * or attributes that conform to the IComplianceFieldAnnotation interface
   */
  static toFieldCompliance(
    annotationOrAttributes: DatasetComplianceAnnotation | IComplianceFieldAnnotation
  ): Com.Linkedin.Dataset.FieldCompliance {
    const annotation =
      annotationOrAttributes instanceof DatasetComplianceAnnotation
        ? annotationOrAttributes
        : new DatasetComplianceAnnotation(annotationOrAttributes);
    const {
      identifierField,
      identifierType,
      logicalType,
      securityClassification,
      valuePattern,
      pii,
      isPurgeKey
    } = annotation;

    return {
      fieldPath: identifierField,
      pegasusFieldPath: '',
      containingPersonalData: pii,
      valuePattern: valuePattern === null ? undefined : valuePattern,
      fieldFormat: logicalType ? logicalType : undefined,
      purgeKey: typeof isPurgeKey === 'boolean' ? isPurgeKey : undefined,
      dataType: (identifierType ? identifierType : undefined) as Com.Linkedin.Dataset.FieldCompliance['dataType'],
      securityClassification: (securityClassification
        ? securityClassification
        : undefined) as Com.Linkedin.Dataset.FieldCompliance['securityClassification'],
      readonly: annotation.hasOwnProperty('readonly')
        ? (annotation as IComplianceFieldAnnotation).readonly
        : (annotation as DatasetComplianceAnnotation).isReadOnly
    };
  }

  constructor(annotationTag?: IComplianceFieldAnnotation, fieldName = '') {
    const data = Object.freeze(annotationTag || complianceAnnotationFactory(fieldName));
    this.data = data;
    this.createWorkingCopy();
  }
}
