import Component from '@ember/component';
import ComputedProperty from '@ember/object/computed';
import { set, get, getProperties, setProperties, computed } from '@ember/object';
import {
  changeSetReviewableAttributeTriggers,
  ComplianceFieldIdValue,
  getDefaultSecurityClassification,
  idTypeTagHasLogicalType,
  isTagIdType,
  NonIdLogicalType,
  tagNeedsReview
} from 'wherehows-web/constants';
import {
  IComplianceChangeSet,
  IComplianceFieldFormatOption,
  IComplianceFieldIdentifierOption
} from 'wherehows-web/typings/app/dataset-compliance';
import { IComplianceDataType } from 'wherehows-web/typings/api/list/compliance-datatypes';
import { action } from '@ember-decorators/object';
import { IdLogicalType } from 'wherehows-web/constants/datasets/compliance';
import { isValidCustomValuePattern } from 'wherehows-web/utils/validators/urn';
import { validateRegExp } from 'wherehows-web/utils/validators/regexp';
import { omit } from 'lodash';
import { arrayMap } from 'wherehows-web/utils/array';

/**
 * Defines the object properties for instances of IQuickDesc
 * @interface IQuickDesc
 */
interface IQuickDesc {
  title: string;
  description?: string;
}

export default class DatasetComplianceFieldTag extends Component {
  classNames = ['dataset-compliance-fields__field-tag'];

  /**
   * External action to update field tag once user updates are complete
   */
  tagDidChange: (sourceTag: IComplianceChangeSet, localTag: IComplianceChangeSet) => void;

  /**
   * References the change set item / tag to be added to the parent field
   * @type {IComplianceChangeSet}
   * @memberof DatasetComplianceFieldTag
   */
  sourceTag: IComplianceChangeSet;

  /**
   * Creates a local copy of the external tag for the related field
   * @type {IComplianceChangeSet}
   * @memberof DatasetComplianceFieldTag
   */
  // ***possibly use oneWay macro***
  tag: ComputedProperty<IComplianceChangeSet> = computed('sourceTag', function(
    this: DatasetComplianceFieldTag
  ): IComplianceChangeSet {
    const source = get(this, 'sourceTag');
    return JSON.parse(JSON.stringify(source));
  });

  /**
   * Flag indicating that the parent field has a single tag associated
   * @type {boolean}
   * @memberof DatasetComplianceFieldTag
   */
  parentHasSingleTag: boolean;

  /**
   * Confidence percentage number used to filter high quality suggestions versus lower quality
   * @type {number}
   * @memberof DatasetComplianceFieldTag
   */
  suggestionConfidenceThreshold: number;

  /**
   * Stores the value of error result if the valuePattern is invalid
   * @type {string}
   */
  valuePatternError: string = '';

  /**
   * References the properties to be shown in the field tag description / help
   * window when an item that has quickDesc properties is interacted with
   * @type {IQuickDesc | null}
   * @memberof DatasetComplianceFieldTag
   */
  fieldIdQuickDesc: IQuickDesc | null = null;

  /**
   * References the field format descriptive properties for the logicalType item,
   * shown in the help window for field format values
   * @type {IQuickDesc | null}
   * @memberof DatasetComplianceFieldTag
   */
  fieldFormatQuickDesc: IQuickDesc | null = null;

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
   * Build the drop down options available for this tag by filtering out options that are not applicable /available for this tag
   * @type {ComputedProperty<Array<IComplianceFieldIdentifierOption>>}
   * @memberof DatasetComplianceFieldTag
   */
  tagIdOptions = computed('hasSingleTag', function(
    this: DatasetComplianceFieldTag
  ): Array<IComplianceFieldIdentifierOption> {
    const { parentHasSingleTag, complianceFieldIdDropdownOptions: allOptions } = getProperties(this, [
      'parentHasSingleTag',
      'complianceFieldIdDropdownOptions'
    ]);

    if (!parentHasSingleTag) {
      const noneOption = allOptions.findBy('value', ComplianceFieldIdValue.None);
      // if the parent field does not have a single tag, then no field can be tagged as ComplianceFieldIdValue.None
      return allOptions.without(noneOption!);
    }

    return allOptions;
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
      return supportedFieldFormats.map(({ id, description }) => ({ value: id, label: id, description }));
    }

    return fieldFormatOptions;
  });

  /**
   * Determines if the CUSTOM input field should be shown for this row's tag
   * @type {ComputedProperty<boolean>}
   */
  showCustomInput = computed('tag.logicalType', function(this: DatasetComplianceFieldTag): boolean {
    const { logicalType, valuePattern } = get(this, 'tag');

    this.actions.tagValuePatternDidChange.call(this, valuePattern || '');

    return logicalType === IdLogicalType.Custom;
  });

  /**
   * Checks if the field format / logical type for this tag is missing, if the field is of ID type
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceFieldTag
   */
  isTagFormatMissing = computed('isIdType', 'tag.logicalType', function(this: DatasetComplianceFieldTag): boolean {
    return get(this, 'isIdType') && !idTypeTagHasLogicalType(get(this, 'tag'));
  });

  /**
   * Determines if the local copy of the tag has diverged from the source
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceFieldTag
   */
  localTagHasDiverged = computed('sourceTag', `tag.{${changeSetReviewableAttributeTriggers}}`, function(
    this: DatasetComplianceFieldTag
  ): boolean {
    const serializedSourceTagValues = Object.values(get(this, 'sourceTag'))
      .sort()
      .toString();
    const serializedLocalTagValues = Object.values(get(this, 'tag'))
      .sort()
      .toString();

    return serializedSourceTagValues !== serializedLocalTagValues;
  });

  /**
   * Determines if the local copy of the tag requires changes before being applied to field tags
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceFieldTag
   */
  isTagReviewRequired = computed(`tag.{${changeSetReviewableAttributeTriggers}}`, 'complianceDataTypes', function(
    this: DatasetComplianceFieldTag
  ): boolean {
    const tagWithoutSuggestion = <IComplianceChangeSet>omit<IComplianceChangeSet>(get(this, 'tag'), ['suggestion']);
    const suggestionConfidenceThreshold = get(this, 'suggestionConfidenceThreshold');

    return tagNeedsReview(get(this, 'complianceDataTypes'), { checkSuggestions: true, suggestionConfidenceThreshold })(
      tagWithoutSuggestion
    );
  });

  /**
   * Applies the argument to the quickDesc property or nullifies if null
   * @param {IQuickDesc | null} quickDesc
   * @param {"fieldId" | "fieldFormat"} type
   */
  setQuickDesc({ quickDesc, type }: { quickDesc: IQuickDesc | null; type: 'fieldId' | 'fieldFormat' }) {
    const quickType = <'fieldIdQuickDesc' | 'fieldFormatQuickDesc'>{
      fieldId: 'fieldIdQuickDesc',
      fieldFormat: 'fieldFormatQuickDesc'
    }[type];

    set(this, quickType, quickDesc);
  }

  /**
   * Sets the value of the pattern error string after p
   * @param {string} errorString
   */
  setPatternErrorString(errorString: string = '') {
    set(this, 'valuePatternError', errorString.replace('SyntaxError: ', ''));
  }

  /**
   * Sets the default classification for the related identifier field's tag
   * Using the identifierType, determine the tag's default security classification based on a values
   * supplied by complianceDataTypes endpoint
   * @param {ComplianceFieldIdValue | NonIdLogicalType | null} identifierType
   */
  setDefaultClassification(identifierType: IComplianceChangeSet['identifierType']): void {
    const complianceDataTypes = get(this, 'complianceDataTypes');
    const defaultSecurityClassification = getDefaultSecurityClassification(complianceDataTypes, identifierType);

    this.changeTagClassification(defaultSecurityClassification);
  }

  /**
   * Updates the security classification on the tag
   * @param {IComplianceChangeSet.securityClassification} securityClassification the updated security classification value
   */
  changeTagClassification(
    this: DatasetComplianceFieldTag,
    securityClassification: IComplianceChangeSet['securityClassification'] = null
  ): void {
    setProperties(get(this, 'tag'), {
      securityClassification,
      isDirty: true
    });
  }

  /**
   * Handles changes to the valuePattern attribute on a tag
   * @param {string} pattern
   * @return {string}
   * @throws {SyntaxError}
   */
  changeTagValuePattern(this: DatasetComplianceFieldTag, pattern: string): string {
    const tag = get(this, 'tag');
    const { isValid } = validateRegExp(pattern, isValidCustomValuePattern);

    set(tag, 'valuePattern', pattern);

    if (!isValid) {
      throw new Error('Pattern not valid');
    }

    return pattern;
  }

  /**
   * Handles UI changes to the tag identifierType
   * @param {ComplianceFieldIdValue} identifierType
   */
  @action
  tagIdentifierTypeDidChange(this: DatasetComplianceFieldTag, identifierType: ComplianceFieldIdValue) {
    const tag = get(this, 'tag');

    // clear out the quickDesc object to allow rendering fieldFormat options
    this.setQuickDesc({ quickDesc: null, type: 'fieldId' });
    setProperties(tag, {
      identifierType,
      logicalType: null,
      nonOwner: null,
      isDirty: true,
      valuePattern: null
    });

    this.setDefaultClassification(identifierType);
  }

  /**
   * Updates the logical type for the related tag
   * @param {IdLogicalType} logicalType contains the selected drop-down value
   */
  @action
  tagLogicalTypeDidChange(this: DatasetComplianceFieldTag, logicalType: IdLogicalType) {
    const tag = get(this, 'tag');
    let properties: Pick<IComplianceChangeSet, 'logicalType' | 'isDirty' | 'valuePattern' | 'nonOwner'> = {
      logicalType,
      isDirty: true,
      // nullifies nonOwner property on logicalType change
      nonOwner: null
    };

    // nullifies valuePattern attr if logicalType is not IdLogicalType.Custom
    if (logicalType === IdLogicalType.Custom) {
      properties = { ...properties, valuePattern: null };
    }

    // clear the current quickDesc item for the Field Format
    this.setQuickDesc({ quickDesc: null, type: 'fieldFormat' });
    setProperties(tag, properties);
  }

  /**
   * Updates the nonOwner property on the tag
   * @param {boolean} nonOwner
   */
  @action
  tagOwnerDidChange(this: DatasetComplianceFieldTag, nonOwner: boolean) {
    setProperties(get(this, 'tag'), {
      nonOwner,
      isDirty: true
    });
  }

  /**
   * Invokes the parent action on user input for value pattern
   * If an exception is thrown, valuePatternError is updated with string value
   * @param {string} pattern user input string
   */
  @action
  tagValuePatternDidChange(this: DatasetComplianceFieldTag, pattern: string) {
    try {
      if (this.changeTagValuePattern(pattern)) {
        //clear pattern error
        this.setPatternErrorString();
      }
    } catch (e) {
      this.setPatternErrorString(e.toString());
    }
  }

  /**
   * Sets the quickDesc property when the onmouseenter event is triggered for a
   * field tag
   * @param {ComplianceFieldIdValue | NonIdLogicalType} value
   */
  @action
  onFieldTagIdentifierEnter(
    this: DatasetComplianceFieldTag,
    { value }: { value: ComplianceFieldIdValue | NonIdLogicalType }
  ) {
    const complianceDataType = get(this, 'complianceDataTypes').findBy('id', value);

    if (complianceDataType) {
      const { title, description } = complianceDataType;
      this.setQuickDesc({ quickDesc: { title, description }, type: 'fieldId' });
    }
  }

  /**
   * Clears the quickDesc property when the onmouseeleave event is
   * triggered for a field tag
   */
  @action
  onFieldTagIdentifierLeave(this: DatasetComplianceFieldTag) {
    this.setQuickDesc({ quickDesc: null, type: 'fieldId' });
  }

  /**
   * Sets the quickDesc object for the field format on hover
   * @param {IdLogicalType} value the field format entry being hovered over
   */
  @action
  onFieldFormatEnter(this: DatasetComplianceFieldTag, { value }: { value: IdLogicalType }) {
    const getSupportedFieldFormats = ({ supportedFieldFormats }: IComplianceDataType) => supportedFieldFormats;
    const supportedFieldFormats = arrayMap(getSupportedFieldFormats)(get(this, 'complianceDataTypes'));
    const fieldFormat = (<Array<{ id: IdLogicalType; description: string }>>[])
      .concat(...supportedFieldFormats)
      .findBy('id', value);

    if (fieldFormat) {
      const { id, description } = fieldFormat;
      this.setQuickDesc({ quickDesc: { title: id, description }, type: 'fieldFormat' });
    }
  }

  /**
   * Clears the quickDesc property when the field format entry is exited
   */
  @action
  onFieldFormatLeave(this: DatasetComplianceFieldTag) {
    this.setQuickDesc({ quickDesc: null, type: 'fieldFormat' });
  }

  /**
   * Invokes the external action to pass on local changes to the source tag
   * @param {() => void} closeDialog
   */
  @action
  onFieldTagUpdate(this: DatasetComplianceFieldTag, closeDialog?: () => void): void {
    this.tagDidChange(get(this, 'sourceTag'), get(this, 'tag'));

    if (typeof closeDialog === 'function') {
      closeDialog();
    }
  }
}
