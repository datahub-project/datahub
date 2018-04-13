import Component from '@ember/component';
import ComputedProperty from '@ember/object/computed';
import { get, getProperties, computed, getWithDefault } from '@ember/object';
import {
  Classification,
  ComplianceFieldIdValue,
  getDefaultSecurityClassification,
  idTypeFieldHasLogicalType,
  isFieldIdType,
  SuggestionIntent
} from 'wherehows-web/constants';
import {
  IComplianceChangeSet,
  IComplianceFieldFormatOption,
  IDropDownOption
} from 'wherehows-web/typings/app/dataset-compliance';
import { IComplianceDataType } from 'wherehows-web/typings/api/list/compliance-datatypes';
import { action } from 'ember-decorators/object';
import { getFieldSuggestions } from 'wherehows-web/utils/datasets/compliance-suggestions';

/**
 * Constant definition for an unselected field format
 * @type {IDropDownOption<null>}
 */
const unSelectedFieldFormatValue: IDropDownOption<null> = {
  value: null,
  label: 'Select Field Format...',
  isDisabled: true
};

export default class DatasetComplianceFieldTag extends Component {
  tagName = 'tr';

  /**
   * Describes action interface for `onSuggestionIntent` action
   * @memberof DatasetComplianceFieldTag
   */
  onSuggestionIntent: (tag: IComplianceChangeSet, intent?: SuggestionIntent) => void;

  /**
   * Describes action interface for `onTagIdentifierTypeChange` action
   * @memberof DatasetComplianceFieldTag
   */
  onTagIdentifierTypeChange: (tag: IComplianceChangeSet, option: { value: ComplianceFieldIdValue | null }) => void;

  /**
   * References the change set item / tag to be added to the parent field
   * @type {IComplianceChangeSet}
   * @memberof DatasetComplianceFieldTag
   */
  tag: IComplianceChangeSet;

  /**
   * Reference to the compliance data types
   * @type {Array<IComplianceDataType>}
   */
  complianceDataTypes: Array<IComplianceDataType>;

  /**
   * Flag indicating that this tag has an identifier type of idType that is true
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceFieldTag
   */
  isIdType: ComputedProperty<boolean> = computed('tag.identifierType', 'complianceDataTypes', function(
    this: DatasetComplianceFieldTag
  ): boolean {
    const { tag, complianceDataTypes } = getProperties(this, ['tag', 'complianceDataTypes']);
    return isFieldIdType(complianceDataTypes)(tag);
  });

  /**
   * A list of field formats that are determined based on the tag identifierType
   * @type ComputedProperty<Array<IComplianceFieldFormatOption>>
   * @memberof DatasetComplianceFieldTag
   */
  fieldFormats: ComputedProperty<Array<IComplianceFieldFormatOption>> = computed(
    'isIdType',
    'complianceDataTypes',
    function(this: DatasetComplianceFieldTag): Array<IComplianceFieldFormatOption> {
      const identifierType = get(this, 'tag')['identifierType'] || '';
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
    }
  );

  /**
   * Checks if the field format / logical type for this tag is missing, if the field is of ID type
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceFieldTag
   */
  isTagFormatMissing = computed('isIdType', 'tag.logicalType', function(this: DatasetComplianceFieldTag): boolean {
    return get(this, 'isIdType') && !idTypeFieldHasLogicalType(get(this, 'tag'));
  });

  /**
   * The tag's security classification
   * Retrieves the tag security classification from the compliance tag if it exists, otherwise
   * defaults to the default security classification for the identifier type
   * in other words, the field must have a security classification if it has an identifier type
   * @type {ComputedProperty<Classification | null>}
   * @memberof DatasetComplianceFieldTag
   */
  tagClassification = computed('tag.classification', 'tag.identifierType', 'complianceDataTypes', function(
    this: DatasetComplianceFieldTag
  ): IComplianceChangeSet['securityClassification'] {
    const { tag: { identifierType, securityClassification }, complianceDataTypes } = getProperties(this, [
      'tag',
      'complianceDataTypes'
    ]);

    return securityClassification || getDefaultSecurityClassification(complianceDataTypes, identifierType);
  });

  /**
   * Flag indicating that this tag has an identifier type that is of pii type
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceFieldTag
   */
  isPiiType = computed('tag.identifierType', function(this: DatasetComplianceFieldTag): boolean {
    const { identifierType } = get(this, 'tag');
    const isDefinedIdentifierType = identifierType !== null || identifierType !== ComplianceFieldIdValue.None;

    // If identifierType exists, and tag is not idType or None or null
    return !!identifierType && !get(this, 'isIdType') && isDefinedIdentifierType;
  });

  /**
   * Extracts the tag suggestions into a cached computed property, if a suggestion exists
   * @type {(ComputedProperty<{ identifierType: ComplianceFieldIdValue; logicalType: string; confidence: number } | void>)}
   * @memberof DatasetComplianceFieldTag
   */
  prediction = computed('tag.suggestion', 'tag.suggestionAuthority', function(
    this: DatasetComplianceFieldTag
  ): {
    identifierType: IComplianceChangeSet['identifierType'];
    logicalType: IComplianceChangeSet['logicalType'];
    confidence: number;
  } | void {
    return getFieldSuggestions(getWithDefault(this, 'tag', <IComplianceChangeSet>{}));
  });

  /**
   * Handles UI changes to the tag identifierType
   * @param {{ value: ComplianceFieldIdValue }} { value }
   */
  @action
  tagIdentifierTypeDidChange(this: DatasetComplianceFieldTag, { value }: { value: ComplianceFieldIdValue | null }) {
    const onTagIdentifierTypeChange = get(this, 'onTagIdentifierTypeChange');

    if (typeof onTagIdentifierTypeChange === 'function') {
      // if the field has a predicted value, but the user changes the identifier type,
      // ignore the suggestion
      if (get(this, 'prediction')) {
        this.onSuggestionAction(SuggestionIntent.ignore);
      }

      onTagIdentifierTypeChange(get(this, 'tag'), { value });
    }
  }

  /**
   * Handles the updates when the tag's logical type changes on this tag
   * @param {(IComplianceChangeSet['logicalType'])} value contains the selected drop-down value
   */
  @action
  tagLogicalTypeDidChange(this: DatasetComplianceFieldTag, { value }: { value: IComplianceChangeSet['logicalType'] }) {
    console.log(value);
  }

  /**
   * Handles UI change to field security classification
   * @param {({ value: '' | Classification })} { value } contains the changed classification value
   */
  @action
  tagClassificationDidChange(this: DatasetComplianceFieldTag, { value }: { value: '' | Classification }) {
    // const onFieldClassificationChange = get(this, 'onFieldClassificationChange');
    // if (typeof onFieldClassificationChange === 'function') {
    //   onFieldClassificationChange(get(this, 'tag'), { value });
    // }
    console.log(value);
  }

  /**
   * Handles the nonOwner flag update on the tag
   * @param {boolean} nonOwner
   */
  @action
  tagOwnerDidChange(this: DatasetComplianceFieldTag, nonOwner: boolean) {
    // get(this, 'onFieldOwnerChange')(get(this, 'field'), nonOwner);
    console.log(nonOwner);
  }

  /**
   * Handler for user interactions with a suggested value. Applies / ignores the suggestion
   * Then invokes the parent supplied suggestion handler
   * @param {SuggestionIntent} intent a binary indicator to accept or ignore suggestion
   */
  @action
  onSuggestionAction(this: DatasetComplianceFieldTag, intent: SuggestionIntent = SuggestionIntent.ignore) {
    const onSuggestionIntent = get(this, 'onSuggestionIntent');

    // Accept the suggestion for either identifierType and/or logicalType
    if (intent === SuggestionIntent.accept) {
      const { identifierType, logicalType } = get(this, 'prediction') || {
        identifierType: void 0,
        logicalType: void 0
      };

      if (identifierType) {
        this.actions.tagIdentifierTypeDidChange.call(this, { value: identifierType });
      }

      if (logicalType) {
        this.actions.tagLogicalTypeDidChange.call(this, logicalType);
      }
    }

    // Invokes parent handle to  runtime ignore future suggesting this suggestion
    if (typeof onSuggestionIntent === 'function') {
      onSuggestionIntent(get(this, 'tag'), intent);
    }
  }
}
