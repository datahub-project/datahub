import DatasetTableRow from 'wherehows-web/components/dataset-table-row';
import ComputedProperty, { alias } from '@ember/object/computed';
import { computed, get, getProperties, getWithDefault } from '@ember/object';
import {
  Classification,
  ComplianceFieldIdValue,
  IComplianceField,
  IFieldIdentifierOption,
  SuggestionIntent,
  getDefaultSecurityClassification,
  IdLogicalType,
  IComplianceFieldFormatOption
} from 'wherehows-web/constants';
import { IComplianceDataType } from 'wherehows-web/typings/api/list/compliance-datatypes';
import { fieldChangeSetRequiresReview } from 'wherehows-web/utils/datasets/compliance-policy';
import { isHighConfidenceSuggestion } from 'wherehows-web/utils/datasets/compliance-suggestions';
import noop from 'wherehows-web/utils/noop';
import { hasEnumerableKeys } from 'wherehows-web/utils/object';

/**
 * Constant definition for an unselected field format
 * @type {object}
 */
const unSelectedFieldFormatValue = { value: null, label: 'Select Field Format...', isDisabled: true };

export default class DatasetComplianceRow extends DatasetTableRow {
  /**
   * Declares the field property on a DatasetTableRow. Contains attributes for a compliance field record
   * @type {IComplianceField}
   * @memberof DatasetComplianceRow
   */
  field: IComplianceField;

  /**
   * Reference to the compliance data types
   * @type {Array<IComplianceDataType>}
   */
  complianceDataTypes: Array<IComplianceDataType>;

  /**
   * Reference to the compliance `onFieldOwnerChange` action
   * @memberof DatasetComplianceRow
   */
  onFieldOwnerChange: (field: IComplianceField, nonOwner: boolean) => void;

  /**
   * Describes action interface for `onFieldIdentifierTypeChange` action
   * @memberof DatasetComplianceRow
   */
  onFieldIdentifierTypeChange: (field: IComplianceField, option: { value: ComplianceFieldIdValue | null }) => void;

  /**
   * Describes action interface for `onFieldLogicalTypeChange` action
   * @memberof DatasetComplianceRow
   */
  onFieldLogicalTypeChange: (field: IComplianceField, value: IComplianceField['logicalType']) => void;

  /**
   * Describes action interface for `onFieldClassificationChange` action
   * 
   * @memberof DatasetComplianceRow
   */
  onFieldClassificationChange: (field: IComplianceField, option: { value: '' | Classification }) => void;

  /**
   * Describes action interface for `onSuggestionIntent` action
   * @memberof DatasetComplianceRow
   */
  onSuggestionIntent: (field: IComplianceField, intent?: SuggestionIntent) => void;

  /**
   * The field identifier attribute
   * @type {ComputedProperty<string>}
   * @memberof DatasetComplianceRow
   */
  identifierField: ComputedProperty<string> = alias('field.identifierField');

  /**
   * Flag indicating if this field is a non owner or owner
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceRow
   */
  nonOwner: ComputedProperty<boolean> = alias('field.nonOwner').readOnly();

  /**
   * The field's dataType attribute
   * @type {ComputedProperty<string>}
   * @memberof DatasetComplianceRow
   */
  dataType: ComputedProperty<string> = alias('field.dataType');

  /**
   * Dropdown options for each compliance field / record
   * @type {Array<IFieldIdentifierOption>}
   * @memberof DatasetComplianceRow
   */
  complianceFieldIdDropdownOptions: Array<IFieldIdentifierOption>;

  /**
   * Reference to the current value of the field's SuggestionIntent if present
   * indicates that the provided suggestion is either accepted or ignored
   * @type {(ComputedProperty<SuggestionIntent | void>)}
   * @memberof DatasetComplianceRow
   */
  suggestionAuthority: ComputedProperty<SuggestionIntent | void> = alias('field.suggestionAuthority');

  /**
   * Maps the suggestion response, if present, to a string resolution
   * @type ComputedProperty<string | void>
   * @memberof DatasetComplianceRow
   */
  suggestionResolution = computed('suggestionAuthority', function(this: DatasetComplianceRow): string | void {
    const suggestionAuthority = get(this, 'suggestionAuthority');

    if (suggestionAuthority) {
      return {
        [SuggestionIntent.accept]: 'Accepted',
        [SuggestionIntent.ignore]: 'Discarded'
      }[suggestionAuthority];
    }
  });

  /**
   * Checks that the field does not have a current policy value
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceRow
   */
  isReviewRequested = computed('field.{isDirty,suggestion,privacyPolicyExists,suggestionAuthority}', function(
    this: DatasetComplianceRow
  ): boolean {
    return fieldChangeSetRequiresReview(get(this, 'field'));
  });

  /**
   * Checks if the field format drop-down should be disabled based on the type of the field
   * *WIP
   * @type {ComputedProperty<void>}
   * @memberof DatasetComplianceRow
   */
  isFieldFormatDisabled = computed('field.identifierType', noop).readOnly();

  /**
   *  Takes a field property and extracts the value on the current policy if a suggestion currently exists for the field
   * @param {('logicalType' | 'identifierType')} fieldProp 
   * @returns {(string | void)} 
   * @memberof DatasetComplianceRow
   */
  getCurrentValueBeforeSuggestion(fieldProp: 'logicalType' | 'identifierType'): string | void {
    /**
     * Current value on policy prior to the suggested value
     * @type {string}
     */
    const value = get(get(this, 'field'), fieldProp);

    if (hasEnumerableKeys(get(this, 'prediction')) && value) {
      /**
       * Field drop down options
       */
      const complianceFieldIdDropdownOptions = get(this, 'complianceFieldIdDropdownOptions');

      /**
       * Convenience function to get `label` attribute on the display properties object
       * @param {(Array<IFieldIdentifierOption> | IFieldIdentifierOption)} [dropDownOptions=[]]
       */
      const getLabel = (dropDownOptions: Array<IFieldIdentifierOption> = []) =>
        ((Array.isArray(dropDownOptions) && dropDownOptions.findBy('value', value)) || { label: void 0 }).label;

      return {
        identifierType: getLabel(complianceFieldIdDropdownOptions),
        logicalType:
          (get(this, 'fieldFormats').find(({ value: format }) => format === value) || { value: void 0 }).value || void 0
      }[fieldProp];
    }
  }

  /**
   * Returns a computed value for the field identifierType
   * @type {ComputedProperty<ComplianceFieldIdValue>}
   * @memberof DatasetComplianceRow
   */
  identifierType = computed('field.identifierType', 'prediction', function(
    this: DatasetComplianceRow
  ): ComplianceFieldIdValue {
    /**
     * Describes the interface for the options bag param passed into the getIdentifierType function below
     * @interface IGetIdentParams
     */
    interface IGetIdentParams {
      identifierType: ComplianceFieldIdValue;
      prediction: { identifierType: ComplianceFieldIdValue } | void;
    }

    const { field: { identifierType }, prediction } = getProperties(this, ['field', 'prediction']);
    /**
     * Inner function takes the field.identifierType and prediction values, and
     * returns the identifierType to be rendered in the ui
     * @param {IGetIdentParams} params 
     * @returns {ComplianceFieldIdValue}
     */
    const getIdentifierType = (params: IGetIdentParams): ComplianceFieldIdValue => {
      const {
        identifierType,
        prediction: { identifierType: suggestedIdentifierType } = { identifierType: void 0 }
      } = params;
      return suggestedIdentifierType || identifierType;
    };

    return getIdentifierType({ identifierType, prediction });
  }).readOnly();

  /**
   * Gets the identifierType on the compliance policy before the suggested value
   * @type {ComputedProperty<string>}
   * @memberof DatasetComplianceRow
   */
  identifierTypeBeforeSuggestion = computed('identifierType', function(): string | void {
    return this.getCurrentValueBeforeSuggestion('identifierType');
  });

  /**
   * Gets the logicalType on the compliance policy before the suggested value
   * @type {ComputedProperty<string>}
   * @memberof DatasetComplianceRow
   */
  logicalTypeBeforeSuggestion = computed('logicalType', function(): string | void {
    return this.getCurrentValueBeforeSuggestion('logicalType');
  });

  /**
   * A list of field formats that are determined based on the field identifierType
   * @type ComputedProperty<Array<IComplianceDataType.supportedFieldFormats> | void>
   * @memberof DatasetComplianceRow
   */
  fieldFormats = computed('isIdType', function(this: DatasetComplianceRow): Array<IComplianceFieldFormatOption> {
    const identifierType: ComplianceFieldIdValue = get(get(this, 'field'), 'identifierType');
    const { isIdType, complianceDataTypes } = getProperties(this, ['isIdType', 'complianceDataTypes']);
    const complianceDataType = complianceDataTypes.findBy('id', identifierType);

    if (complianceDataType && isIdType) {
      const supportedFieldFormats = complianceDataType.supportedFieldFormats || [];
      const fieldFormatOptions = supportedFieldFormats.map(format => ({ value: format, label: format }));

      return fieldFormatOptions.length > 1 ? [unSelectedFieldFormatValue, ...fieldFormatOptions] : fieldFormatOptions;
    }

    return [];
  });

  /**
   * Flag indicating that this field has an identifier type of idType that is true
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceRow
   */
  isIdType: ComputedProperty<boolean> = computed('field.identifierType', function(this: DatasetComplianceRow): boolean {
    const identifierType: ComplianceFieldIdValue = get(get(this, 'field'), 'identifierType');
    const complianceDataTypes = get(this, 'complianceDataTypes');
    const complianceDataType = complianceDataTypes.findBy('id', identifierType) || { idType: false };

    return complianceDataType.idType;
  });

  /**
   * Flag indicating that this field has an identifier type that is of pii type
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceRow
   */
  isPiiType = computed('field.identifierType', function(this: DatasetComplianceRow): boolean {
    const identifierType: ComplianceFieldIdValue = get(get(this, 'field'), 'identifierType');

    return !get(this, 'isIdType') && ![ComplianceFieldIdValue.None, null].includes(identifierType);
  });

  /**
   * The fields logical type, rendered as an Object
   * If a prediction exists for this field, the predicted value is shown instead
   * @type {(ComputedProperty<IFieldFormatDropdownOption | void>)}
   * @memberof DatasetComplianceRow
   */
  logicalType = computed('field.logicalType', 'prediction', function(
    this: DatasetComplianceRow
  ): IComplianceField['logicalType'] {
    const {
      field: { logicalType },
      prediction: { logicalType: suggestedLogicalType } = { logicalType: null }
    } = getProperties(this, ['field', 'prediction']);

    return suggestedLogicalType || logicalType;
  });

  /**
   * The field's security classification
   * Retrieves the field security classification from the compliance field if it exists, otherwise
   * defaults to the default security classification for the identifier type
   * in other words, the field must have a security classification if it has an identifier type
   * @type {ComputedProperty<Classification | null>}
   * @memberof DatasetComplianceRow
   */
  classification = computed('field.classification', 'field.identifierType', 'complianceDataTypes', function(
    this: DatasetComplianceRow
  ): Classification | null {
    const { field: { identifierType, classification }, complianceDataTypes } = getProperties(this, [
      'field',
      'complianceDataTypes'
    ]);

    return classification || getDefaultSecurityClassification(complianceDataTypes, identifierType);
  });

  /**
   * Extracts the field suggestions into a cached computed property, if a suggestion exists
   * @type {(ComputedProperty<{ identifierType: ComplianceFieldIdValue; logicalType: string; confidence: number } | void>)}
   * @memberof DatasetComplianceRow
   */
  prediction = computed('field.suggestion', 'field.suggestionAuthority', function(
    this: DatasetComplianceRow
  ): {
    identifierType: ComplianceFieldIdValue;
    logicalType: IComplianceField['logicalType'];
    confidence: number;
  } | void {
    const field = getWithDefault(this, 'field', <IComplianceField>{});
    // If a suggestionAuthority property exists on the field, then the user has already either accepted or ignored
    // the suggestion for this field. It's value should not be taken into account on re-renders
    // in place, this substitutes an empty suggestion
    const { suggestion } = field.hasOwnProperty('suggestionAuthority') ? { suggestion: void 0 } : field;

    if (suggestion && isHighConfidenceSuggestion(suggestion)) {
      const { identifierType, logicalType, confidenceLevel: confidence } = suggestion;

      return { identifierType, logicalType, confidence: +(confidence * 100).toFixed(2) };
    }
  });

  actions = {
    /**
     * Handles UI changes to the field identifierType
     * @param {{ value: ComplianceFieldIdValue }} { value }
     */
    onFieldIdentifierTypeChange(this: DatasetComplianceRow, { value }: { value: ComplianceFieldIdValue | null }) {
      const onFieldIdentifierTypeChange = get(this, 'onFieldIdentifierTypeChange');
      if (typeof onFieldIdentifierTypeChange === 'function') {
        onFieldIdentifierTypeChange(get(this, 'field'), { value });
      }
    },

    /**
     * Handles the updates when the field logical type changes on this field
     * @param {(IComplianceField['logicalType'])} value contains the selected drop-down value
     */
    onFieldLogicalTypeChange(this: DatasetComplianceRow, { value }: { value: IComplianceField['logicalType'] | null }) {
      const onFieldLogicalTypeChange = get(this, 'onFieldLogicalTypeChange');
      if (typeof onFieldLogicalTypeChange === 'function') {
        onFieldLogicalTypeChange(get(this, 'field'), value);
      }
    },

    /**
     * Handles UI change to field security classification
     * @param {({ value: '' | Classification })} { value } contains the changed classification value
     */
    onFieldClassificationChange(this: DatasetComplianceRow, { value }: { value: '' | Classification }) {
      const onFieldClassificationChange = get(this, 'onFieldClassificationChange');
      if (typeof onFieldClassificationChange === 'function') {
        onFieldClassificationChange(get(this, 'field'), { value });
      }
    },

    /**
     * Handles the nonOwner flag update on the field
     * @param {boolean} nonOwner
     */
    onOwnerChange(this: DatasetComplianceRow, nonOwner: boolean) {
      get(this, 'onFieldOwnerChange')(get(this, 'field'), nonOwner);
    },

    /**
     * Handler for user interactions with a suggested value. Applies / ignores the suggestion
     * Then invokes the parent supplied suggestion handler
     * @param {string | void} intent a binary indicator to accept or ignore suggestion
     * @param {SuggestionIntent} intent 
     */
    onSuggestionAction(this: DatasetComplianceRow, intent?: SuggestionIntent) {
      const onSuggestionIntent = get(this, 'onSuggestionIntent');

      // Accept the suggestion for either identifierType and/or logicalType
      if (intent === SuggestionIntent.accept) {
        const { identifierType, logicalType } = get(this, 'prediction') || {
          identifierType: void 0,
          logicalType: void 0
        };

        if (identifierType) {
          this.actions.onFieldIdentifierTypeChange.call(this, { value: identifierType });
        }

        if (logicalType) {
          this.actions.onFieldLogicalTypeChange.call(this, logicalType);
        }
      }

      // Invokes parent handle to  runtime ignore future suggesting this suggestion
      if (typeof onSuggestionIntent === 'function') {
        onSuggestionIntent(get(this, 'field'), intent);
      }
    }
  };
}
