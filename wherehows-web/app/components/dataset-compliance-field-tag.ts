import Component from '@ember/component';
import ComputedProperty from '@ember/object/computed';
import { get, getProperties, computed } from '@ember/object';
import { ComplianceFieldIdValue, idTypeFieldHasLogicalType, isTagIdType } from 'wherehows-web/constants';
import {
  IComplianceChangeSet,
  IComplianceFieldFormatOption,
  IComplianceFieldIdentifierOption,
  IDropDownOption
} from 'wherehows-web/typings/app/dataset-compliance';
import { IComplianceDataType } from 'wherehows-web/typings/api/list/compliance-datatypes';
import { action } from 'ember-decorators/object';

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
   * Describes action interface for `onTagIdentifierTypeChange` action
   * @memberof DatasetComplianceFieldTag
   */
  onTagIdentifierTypeChange: (tag: IComplianceChangeSet, option: { value: ComplianceFieldIdValue | null }) => void;

  /**
   * Describes the parent action interface for `onTagLogicalTypeChange`
   */
  onTagLogicalTypeChange: (tag: IComplianceChangeSet, value: IComplianceChangeSet['logicalType']) => void;

  /**
   * Describes the parent action interface for `onTagOwnerChange`
   */
  onTagOwnerChange: (tag: IComplianceChangeSet, nonOwner: boolean) => void;

  /**
   * References the change set item / tag to be added to the parent field
   * @type {IComplianceChangeSet}
   * @memberof DatasetComplianceFieldTag
   */
  tag: IComplianceChangeSet;

  /**
   * Flag indicating that the parent field has a single tag associated
   * @type {boolean}
   * @memberof DatasetComplianceFieldTag
   */
  parentHasSingleTag: boolean;

  /**
   * Reference to the compliance data types
   * @type {Array<IComplianceDataType>}
   */
  complianceDataTypes: Array<IComplianceDataType>;

  /**
   * Reference to the full list of options for the identifierType tag property IComplianceFieldIdentifierOption
   * @type {Array<IComplianceFieldIdentifierOption>}
   */
  complianceFieldIdDropdownOptions: Array<IComplianceFieldIdentifierOption>;

  /**
   * Build the dropdown options available for this tag by filtering out options that are not applicable /available for this tag
   * @type {ComputedProperty<Array<IComplianceFieldIdentifierOption>>}
   * @memberof DatasetComplianceFieldTag
   */
  fieldIdDropDownOptions = computed('hasSingleTag', function(
    this: DatasetComplianceFieldTag
  ): Array<IComplianceFieldIdentifierOption> {
    const { parentHasSingleTag, complianceFieldIdDropdownOptions: dropDownOptions } = getProperties(this, [
      'parentHasSingleTag',
      'complianceFieldIdDropdownOptions'
    ]);

    // if the parent field does not have a single tag, then no field can be tagged as ComplianceFieldIdValue.None
    if (!parentHasSingleTag) {
      const NoneOption = dropDownOptions.findBy('value', ComplianceFieldIdValue.None);
      return dropDownOptions.without(NoneOption!);
    }

    return dropDownOptions;
  });

  /**
   * Flag indicating that this tag has an identifier type of idType that is true
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceFieldTag
   */
  isIdType: ComputedProperty<boolean> = computed('tag.identifierType', 'complianceDataTypes', function(
    this: DatasetComplianceFieldTag
  ): boolean {
    const { tag, complianceDataTypes } = getProperties(this, ['tag', 'complianceDataTypes']);
    return isTagIdType(complianceDataTypes)(tag);
  });

  /**
   * A list of field formats that are determined based on the tag identifierType
   * @type ComputedProperty<Array<IComplianceFieldFormatOption>>
   * @memberof DatasetComplianceFieldTag
   */
  fieldFormats: ComputedProperty<Array<IComplianceFieldFormatOption>> = computed('isIdType', function(
    this: DatasetComplianceFieldTag
  ): Array<IComplianceFieldFormatOption> {
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
  });

  /**
   * Checks if the field format / logical type for this tag is missing, if the field is of ID type
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceFieldTag
   */
  isTagFormatMissing = computed('isIdType', 'tag.logicalType', function(this: DatasetComplianceFieldTag): boolean {
    return get(this, 'isIdType') && !idTypeFieldHasLogicalType(get(this, 'tag'));
  });

  /**
   * Handles UI changes to the tag identifierType
   * @param {{ value: ComplianceFieldIdValue }} { value }
   */
  @action
  tagIdentifierTypeDidChange(this: DatasetComplianceFieldTag, { value }: { value: ComplianceFieldIdValue | null }) {
    const onTagIdentifierTypeChange = get(this, 'onTagIdentifierTypeChange');

    if (typeof onTagIdentifierTypeChange === 'function') {
      onTagIdentifierTypeChange(get(this, 'tag'), { value });
    }
  }

  /**
   * Handles the updates when the tag's logical type changes on this tag
   * @param {(IComplianceChangeSet['logicalType'])} value contains the selected drop-down value
   */
  @action
  tagLogicalTypeDidChange(this: DatasetComplianceFieldTag, { value }: { value: IComplianceChangeSet['logicalType'] }) {
    const onTagLogicalTypeChange = get(this, 'onTagLogicalTypeChange');

    if (typeof onTagLogicalTypeChange === 'function') {
      onTagLogicalTypeChange(get(this, 'tag'), value);
    }
  }

  /**
   * Handles the nonOwner flag update on the tag
   * @param {boolean} nonOwner
   */
  @action
  tagOwnerDidChange(this: DatasetComplianceFieldTag, nonOwner: boolean) {
    // inverts the value of nonOwner, toggle is shown in the UI as `Owner` i.e. not nonOwner
    get(this, 'onTagOwnerChange')(get(this, 'tag'), !nonOwner);
  }
}
