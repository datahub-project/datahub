import DatasetTableRow from 'wherehows-web/components/dataset-table-row';
import ComputedProperty, { alias } from '@ember/object/computed';
import { computed, get, getProperties, getWithDefault } from '@ember/object';
import {
  Classification,
  defaultFieldDataTypeClassification,
  fieldIdentifierTypeIds,
  ComplianceFieldIdValue,
  hasPredefinedFieldFormat,
  IComplianceField,
  IFieldIdentifierOption,
  isCustomId,
  isMixedId,
  logicalTypesForGeneric,
  logicalTypesForIds,
  SuggestionIntent,
  IFieldFormatDropdownOption
} from 'wherehows-web/constants';
import { fieldChangeSetRequiresReview } from 'wherehows-web/utils/datasets/compliance-policy';
import { isHighConfidenceSuggestion } from 'wherehows-web/utils/datasets/compliance-suggestions';
import { hasEnumerableKeys } from 'wherehows-web/utils/object';

export default class DatasetComplianceRow extends DatasetTableRow {
  /**
   * Declares the field property on a DatasetTableRow. Contains attributes for a compliance field record
   * @type {IComplianceField}
   * @memberof DatasetComplianceRow
   */
  field: IComplianceField;

  /**
   * Describes action interface for `onFieldIdentifierTypeChange` action
   * @memberof DatasetComplianceRow
   */
  onFieldIdentifierTypeChange: (field: IComplianceField, option: { value: ComplianceFieldIdValue }) => void;

  /**
   * Describes action interface for `onFieldLogicalTypeChange` action
   * @memberof DatasetComplianceRow
   */
  onFieldLogicalTypeChange: (
    field: IComplianceField,
    option: { value: void | IFieldFormatDropdownOption['value'] }
  ) => void;

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
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceRow
   */
  isFieldFormatDisabled = computed('field.identifierType', function(this: DatasetComplianceRow): boolean {
    return hasPredefinedFieldFormat(get(get(this, 'field'), 'identifierType'));
  }).readOnly();

  /**
   *  Takes a field property and extracts the value on the current policy if a suggestion currently exists for the field
   * @param {('logicalType' | 'identifierType')} fieldProp 
   * @returns {(string | void)} 
   * @memberof DatasetComplianceRow
   */
  getCurrentValueBeforeSuggestion(fieldProp: 'logicalType' | 'identifierType'): string | void {
    if (hasEnumerableKeys(get(this, 'prediction'))) {
      /**
       * Current value on policy prior to the suggested value
       * @type {string}
       */
      const value = get(get(this, 'field'), fieldProp);
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
        logicalType: getLabel(get(this, 'fieldFormats'))
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
   * @type ComputedProperty<Array<IFieldFormatDropdownOption> | void>
   * @memberof DatasetComplianceRow
   */
  fieldFormats = computed('field.identifierType', function(
    this: DatasetComplianceRow
  ): Array<IFieldFormatDropdownOption> | undefined {
    const identifierType: ComplianceFieldIdValue = get(get(this, 'field'), 'identifierType');
    const urnFieldFormat: IFieldFormatDropdownOption | void = logicalTypesForIds.findBy('value', 'URN');
    const fieldFormats: Array<IFieldFormatDropdownOption> = fieldIdentifierTypeIds.includes(identifierType)
      ? logicalTypesForIds
      : logicalTypesForGeneric;

    if (isMixedId(identifierType)) {
      return urnFieldFormat ? [urnFieldFormat] : void 0;
    }

    if (isCustomId(identifierType)) {
      return;
    }

    return fieldFormats;
  });

  /**
   * The fields logical type, rendered as an Object
   * If a prediction exists for this field, the predicted value is shown instead
   * @type {(ComputedProperty<IFieldFormatDropdownOption | void>)}
   * @memberof DatasetComplianceRow
   */
  logicalType = computed('field.logicalType', 'prediction', 'fieldFormats', function(
    this: DatasetComplianceRow
  ): IFieldFormatDropdownOption | void {
    let {
      field: { logicalType },
      fieldFormats,
      prediction: { logicalType: suggestedLogicalType } = { logicalType: void 0 }
    } = getProperties(this, ['field', 'fieldFormats', 'prediction']);

    suggestedLogicalType && (logicalType = suggestedLogicalType);

    // Same object reference for equality comparision
    return Array.isArray(fieldFormats) ? fieldFormats.findBy('value', logicalType) : fieldFormats;
  });

  /**
   * The field security classification
   * @type {ComputedProperty<Classification>}
   * @memberof DatasetComplianceRow
   */
  classification = computed('field.classification', 'field.identifierType', function(
    this: DatasetComplianceRow
  ): Classification {
    const identifierType = get(get(this, 'field'), 'identifierType');
    const mixed = isMixedId(identifierType);
    // Filtered list of id logical types that end with urn, or have no value
    const urnFieldFormat = logicalTypesForIds.findBy('value', 'URN');

    return (
      get(get(this, 'field'), 'classification') ||
      (mixed && urnFieldFormat && defaultFieldDataTypeClassification[urnFieldFormat.value])
    );
  });

  /**
   * Extracts the field suggestions into a cached computed property, if a suggestion exists
   * @type {(ComputedProperty<{ identifierType: ComplianceFieldIdValue; logicalType: string; confidence: number } | void>)}
   * @memberof DatasetComplianceRow
   */
  prediction = computed('field.suggestion', 'field.suggestionAuthority', function(
    this: DatasetComplianceRow
  ): { identifierType: ComplianceFieldIdValue; logicalType: string; confidence: number } | void {
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

  /**
   * 
   * @memberof DatasetComplianceRow
   */
  actions = {
    /**
     * Handles UI changes to the field identifierType
     * @param {{ value: ComplianceFieldIdValue }} { value }
     */
    onFieldIdentifierTypeChange(this: DatasetComplianceRow, { value }: { value: ComplianceFieldIdValue }) {
      const onFieldIdentifierTypeChange = get(this, 'onFieldIdentifierTypeChange');
      if (typeof onFieldIdentifierTypeChange === 'function') {
        onFieldIdentifierTypeChange(get(this, 'field'), { value });
      }
    },

    /**
     * Handles the updates when the field logical type changes on this field
     * @param {IFieldFormatDropdownOption} option contains the selected dropdown value
     */
    onFieldLogicalTypeChange(this: DatasetComplianceRow, option: IFieldFormatDropdownOption | null) {
      const { value } = option || { value: void 0 };
      const onFieldLogicalTypeChange = get(this, 'onFieldLogicalTypeChange');
      if (typeof onFieldLogicalTypeChange === 'function') {
        onFieldLogicalTypeChange(get(this, 'field'), { value });
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
          this.actions.onFieldLogicalTypeChange.call(this, { value: logicalType });
        }
      }

      // Invokes parent handle to  runtime ignore future suggesting this suggestion
      if (typeof onSuggestionIntent === 'function') {
        onSuggestionIntent(get(this, 'field'), intent);
      }
    }
  };
}
