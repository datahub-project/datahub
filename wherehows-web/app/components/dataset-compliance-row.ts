import { action } from 'ember-decorators/object';
import { IComplianceChangeSet } from 'wherehows-web/components/dataset-compliance';
import DatasetTableRow from 'wherehows-web/components/dataset-table-row';
import ComputedProperty, { alias, bool } from '@ember/object/computed';
import { computed, get, getProperties, getWithDefault } from '@ember/object';
import {
  Classification,
  ComplianceFieldIdValue,
  SuggestionIntent,
  getDefaultSecurityClassification,
  IComplianceFieldFormatOption,
  IComplianceFieldIdentifierOption,
  IFieldIdentifierOption,
  fieldChangeSetRequiresReview,
  isFieldIdType,
  changeSetReviewableAttributeTriggers,
  idTypeFieldHasLogicalType
} from 'wherehows-web/constants';
import { IComplianceDataType } from 'wherehows-web/typings/api/list/compliance-datatypes';
import { getFieldSuggestions } from 'wherehows-web/utils/datasets/compliance-suggestions';
import noop from 'wherehows-web/utils/noop';
import { hasEnumerableKeys } from 'wherehows-web/utils/object';
import { IComplianceEntity } from 'wherehows-web/typings/api/datasets/compliance';

/**
 * Constant definition for an unselected field format
 * @type {IFieldIdentifierOption<null>}
 */
const unSelectedFieldFormatValue: IFieldIdentifierOption<null> = {
  value: null,
  label: 'Select Field Format...',
  isDisabled: true
};

export default class DatasetComplianceRow extends DatasetTableRow {
  classNameBindings = ['isReadonlyEntity:dataset-compliance-fields--readonly'];

  /**
   * Declares the field property on a DatasetTableRow. Contains attributes for a compliance field record
   * @type {IComplianceChangeSet}
   * @memberof DatasetComplianceRow
   */
  field: IComplianceChangeSet;

  /**
   * Reference to the compliance data types
   * @type {Array<IComplianceDataType>}
   */
  complianceDataTypes: Array<IComplianceDataType>;

  /**
   * Reference to the compliance `onFieldOwnerChange` action
   * @memberof DatasetComplianceRow
   */
  onFieldOwnerChange: (field: IComplianceChangeSet, nonOwner: boolean) => void;

  /**
   * Describes action interface for `onFieldIdentifierTypeChange` action
   * @memberof DatasetComplianceRow
   */
  onFieldIdentifierTypeChange: (field: IComplianceChangeSet, option: { value: ComplianceFieldIdValue | null }) => void;

  /**
   * Describes action interface for `onFieldLogicalTypeChange` action
   * @memberof DatasetComplianceRow
   */
  onFieldLogicalTypeChange: (field: IComplianceChangeSet, value: IComplianceChangeSet['logicalType']) => void;

  /**
   * Describes action interface for `onFieldClassificationChange` action
   * @memberof DatasetComplianceRow
   */
  onFieldClassificationChange: (field: IComplianceChangeSet, option: { value: '' | Classification }) => void;

  /**
   * Describes action interface for `onSuggestionIntent` action
   * @memberof DatasetComplianceRow
   */
  onSuggestionIntent: (field: IComplianceChangeSet, intent?: SuggestionIntent) => void;

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
  nonOwner: ComputedProperty<IComplianceEntity['nonOwner']> = alias('field.nonOwner').readOnly();

  /**
   * The field's dataType attribute
   * @type {ComputedProperty<string>}
   * @memberof DatasetComplianceRow
   */
  dataType: ComputedProperty<string> = alias('field.dataType');

  /**
   * Aliases a fields readonly attribute as a boolean computed property
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceRow
   */
  isReadonlyEntity: ComputedProperty<boolean> = bool('field.readonly');

  /**
   * Dropdown options for each compliance field / record
   * @type {Array<IComplianceFieldIdentifierOption>}
   * @memberof DatasetComplianceRow
   */
  complianceFieldIdDropdownOptions: Array<IComplianceFieldIdentifierOption>;

  /**
   * Reference to the current value of the field's SuggestionIntent if present
   * indicates that the provided suggestion is either accepted or ignored
   * @type {(ComputedProperty<SuggestionIntent | void>)}
   * @memberof DatasetComplianceRow
   */
  suggestionAuthority: ComputedProperty<IComplianceChangeSet['suggestionAuthority']> = alias(
    'field.suggestionAuthority'
  );

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
  isReviewRequested = computed(`field.{${changeSetReviewableAttributeTriggers}}`, 'complianceDataTypes', function(
    this: DatasetComplianceRow
  ): boolean {
    return fieldChangeSetRequiresReview(get(this, 'complianceDataTypes'))(get(this, 'field'));
  });

  /**
   * Checks if the field format / logical type for this field if missing if the field is of ID type
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceRow
   */
  isFieldFormatMissing = computed('isIdType', 'field.logicalType', function(): boolean {
    return get(this, 'isIdType') && !idTypeFieldHasLogicalType(get(this, 'field'));
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
   * @returns {(string | null)}
   * @memberof DatasetComplianceRow
   */
  getCurrentValueBeforeSuggestion(
    this: DatasetComplianceRow,
    fieldProp: 'logicalType' | 'identifierType'
  ): string | null {
    /**
     * Current value on policy prior to the suggested value
     * @type {string}
     */
    const value = get(this, 'field')[fieldProp];

    if (hasEnumerableKeys(get(this, 'prediction')) && value) {
      const { label: currentIdType } = getWithDefault(this, 'complianceFieldIdDropdownOptions', []).findBy(
        'value',
        value
      ) || { label: null };

      const { value: currentLogicalType }: Pick<IComplianceFieldFormatOption, 'value'> = getWithDefault(
        this,
        'fieldFormats',
        []
      ).findBy('value', value) || { value: null };

      return {
        identifierType: currentIdType,
        logicalType: currentLogicalType
      }[fieldProp];
    }

    return null;
  }

  /**
   * Returns a computed value for the field identifierType
   * @type {ComputedProperty<IComplianceChangeSet['identifierType']>}
   * @memberof DatasetComplianceRow
   */
  identifierType = computed('field.identifierType', 'prediction', function(
    this: DatasetComplianceRow
  ): IComplianceChangeSet['identifierType'] {
    /**
     * Describes the interface for the options bag param passed into the getIdentifierType function below
     * @interface IGetIdentParams
     */
    interface IGetIdentParams {
      identifierType: IComplianceChangeSet['identifierType'];
      prediction: { identifierType: IComplianceChangeSet['identifierType'] } | void;
    }

    const { field: { identifierType }, prediction } = getProperties(this, ['field', 'prediction']);
    /**
     * Inner function takes the field.identifierType and prediction values, and
     * returns the identifierType to be rendered in the ui
     * @param {IGetIdentParams} params
     * @returns {IComplianceChangeSet.identifierType}
     */
    const getIdentifierType = (params: IGetIdentParams): IComplianceChangeSet['identifierType'] => {
      const { identifierType, prediction } = params;
      return prediction ? prediction.identifierType : identifierType;
    };

    return getIdentifierType({ identifierType, prediction });
  }).readOnly();

  /**
   * Gets the identifierType on the compliance policy before the suggested value
   * @type {ComputedProperty<string | null>}
   * @memberof DatasetComplianceRow
   */
  identifierTypeBeforeSuggestion = computed('identifierType', function(): string | null {
    return this.getCurrentValueBeforeSuggestion('identifierType');
  });

  /**
   * Gets the logicalType on the compliance policy before the suggested value
   * @type {ComputedProperty<string | null>}
   * @memberof DatasetComplianceRow
   */
  logicalTypeBeforeSuggestion = computed('logicalType', function(): string | null {
    return this.getCurrentValueBeforeSuggestion('logicalType');
  });

  /**
   * A list of field formats that are determined based on the field identifierType
   * @type ComputedProperty<Array<IComplianceFieldFormatOption>>
   * @memberof DatasetComplianceRow
   */
  fieldFormats = computed('isIdType', 'complianceDataTypes', function(
    this: DatasetComplianceRow
  ): Array<IComplianceFieldFormatOption> {
    const identifierType = get(this, 'field')['identifierType'] || '';
    const { isIdType, complianceDataTypes } = getProperties(this, ['isIdType', 'complianceDataTypes']);
    const complianceDataType = complianceDataTypes.findBy('id', identifierType);
    let fieldFormatOptions: Array<IComplianceFieldFormatOption> = [];

    if (complianceDataType && isIdType) {
      const supportedFieldFormats = complianceDataType.supportedFieldFormats || [];
      const supportedFormatOptions = supportedFieldFormats.map(format => ({ value: format, label: format }));

      return supportedFormatOptions.length
        ? [unSelectedFieldFormatValue, ...supportedFormatOptions]
        : supportedFormatOptions;
    }

    return fieldFormatOptions;
  });

  /**
   * Flag indicating that this field has an identifier type of idType that is true
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceRow
   */
  isIdType: ComputedProperty<boolean> = computed('field.identifierType', 'complianceDataTypes', function(
    this: DatasetComplianceRow
  ): boolean {
    const { field, complianceDataTypes } = getProperties(this, ['field', 'complianceDataTypes']);
    return isFieldIdType(complianceDataTypes)(field);
  });

  /**
   * Flag indicating that this field has an identifier type that is of pii type
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceRow
   */
  isPiiType = computed('field.identifierType', function(this: DatasetComplianceRow): boolean {
    const { identifierType } = get(this, 'field');
    const isDefinedIdentifierType = identifierType !== null || identifierType !== ComplianceFieldIdValue.None;

    // If identifierType exists, and field is not idType or None or null
    return !!identifierType && !get(this, 'isIdType') && isDefinedIdentifierType;
  });

  /**
   * The fields logical type, rendered as an Object
   * If a prediction exists for this field, the predicted value is shown instead
   * @type {(ComputedProperty<IComplianceChangeSet.logicalType>)}
   * @memberof DatasetComplianceRow
   */
  logicalType = computed('field.logicalType', 'prediction', function(
    this: DatasetComplianceRow
  ): IComplianceChangeSet['logicalType'] {
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
  ): IComplianceChangeSet['securityClassification'] {
    const { field: { identifierType, securityClassification }, complianceDataTypes } = getProperties(this, [
      'field',
      'complianceDataTypes'
    ]);

    return securityClassification || getDefaultSecurityClassification(complianceDataTypes, identifierType);
  });

  /**
   * Extracts the field suggestions into a cached computed property, if a suggestion exists
   * @type {(ComputedProperty<{ identifierType: ComplianceFieldIdValue; logicalType: string; confidence: number } | void>)}
   * @memberof DatasetComplianceRow
   */
  prediction = computed('field.suggestion', 'field.suggestionAuthority', function(
    this: DatasetComplianceRow
  ): {
    identifierType: IComplianceChangeSet['identifierType'];
    logicalType: IComplianceChangeSet['logicalType'];
    confidence: number;
  } | void {
    return getFieldSuggestions(getWithDefault(this, 'field', <IComplianceChangeSet>{}));
  });

  /**
   * Handles UI changes to the field identifierType
   * @param {{ value: ComplianceFieldIdValue }} { value }
   */
  @action
  fieldIdentifierTypeDidChange(this: DatasetComplianceRow, { value }: { value: ComplianceFieldIdValue | null }) {
    const onFieldIdentifierTypeChange = get(this, 'onFieldIdentifierTypeChange');
    if (typeof onFieldIdentifierTypeChange === 'function') {
      // if the field has a predicted value, but the user changes the identifier type,
      // ignore the suggestion
      if (get(this, 'prediction')) {
        this.onSuggestionAction(SuggestionIntent.ignore);
      }

      onFieldIdentifierTypeChange(get(this, 'field'), { value });
    }
  }

  /**
   * Handles the updates when the field logical type changes on this field
   * @param {(IComplianceChangeSet['logicalType'])} value contains the selected drop-down value
   */
  @action
  fieldLogicalTypeDidChange(this: DatasetComplianceRow, { value }: { value: IComplianceChangeSet['logicalType'] }) {
    const onFieldLogicalTypeChange = get(this, 'onFieldLogicalTypeChange');
    if (typeof onFieldLogicalTypeChange === 'function') {
      onFieldLogicalTypeChange(get(this, 'field'), value);
    }
  }

  /**
   * Handles UI change to field security classification
   * @param {({ value: '' | Classification })} { value } contains the changed classification value
   */
  @action
  fieldClassificationDidChange(this: DatasetComplianceRow, { value }: { value: '' | Classification }) {
    const onFieldClassificationChange = get(this, 'onFieldClassificationChange');
    if (typeof onFieldClassificationChange === 'function') {
      onFieldClassificationChange(get(this, 'field'), { value });
    }
  }

  /**
   * Handles the nonOwner flag update on the field
   * @param {boolean} nonOwner
   */
  @action
  onOwnerChange(this: DatasetComplianceRow, nonOwner: boolean) {
    get(this, 'onFieldOwnerChange')(get(this, 'field'), nonOwner);
  }

  /**
   * Handler for user interactions with a suggested value. Applies / ignores the suggestion
   * Then invokes the parent supplied suggestion handler
   * @param {SuggestionIntent} intent a binary indicator to accept or ignore suggestion
   */
  @action
  onSuggestionAction(this: DatasetComplianceRow, intent: SuggestionIntent = SuggestionIntent.ignore) {
    const onSuggestionIntent = get(this, 'onSuggestionIntent');

    // Accept the suggestion for either identifierType and/or logicalType
    if (intent === SuggestionIntent.accept) {
      const { identifierType, logicalType } = get(this, 'prediction') || {
        identifierType: void 0,
        logicalType: void 0
      };

      if (identifierType) {
        this.actions.fieldIdentifierTypeDidChange.call(this, { value: identifierType });
      }

      if (logicalType) {
        this.actions.fieldLogicalTypeDidChange.call(this, logicalType);
      }
    }

    // Invokes parent handle to  runtime ignore future suggesting this suggestion
    if (typeof onSuggestionIntent === 'function') {
      onSuggestionIntent(get(this, 'field'), intent);
    }
  }
}
