import Ember from 'ember';
import { capitalize } from '@ember/string';
import {
  Classification,
  nonIdFieldLogicalTypes,
  NonIdLogicalType,
  idLogicalTypes,
  genericLogicalTypes,
  fieldIdentifierTypes,
  IdLogicalType,
  ComplianceFieldIdValue
} from 'wherehows-web/constants/datasets/compliance';
import { IComplianceDataType } from 'wherehows-web/typings/api/list/compliance-datatypes';

/**
 * Defines the interface for an each security classification dropdown option
 * @export
 * @interface ISecurityClassificationOption
 */
export interface ISecurityClassificationOption {
  value: '' | Classification;
  label: string;
}

/**
 * Describes the interface for a drop down option for the field format column in the compliance table
 * 
 * @export
 * @interface IFieldFormatDropdownOption
 */
export interface IFieldFormatDropdownOption {
  value: IdLogicalType | NonIdLogicalType;
  label: string;
}

/**
 * Length of time between suggestion modification time and last modified time for the compliance policy
 * If a policy has been updated within the range of this window then it is considered as stale / or
 * has been seen previously
 * @type {number}
 */
const lastSeenSuggestionInterval: number = 7 * 24 * 60 * 60 * 1000;

/**
 * Percentage value for a compliance policy suggestion with a low confidence score
 * @type {number}
 */
const lowQualitySuggestionConfidenceThreshold = 0.5;

/**
 * Stores a unique list of classification values
 * @type {Array<Classification>} the list of classification values
 */
const classifiers = Object.values(Classification);

/**
 * Takes a string, returns a formatted string. Niche , single use case
 * for now, so no need to make into a helper
 * @param {string} string
 */
const formatAsCapitalizedStringWithSpaces = (string: string) => capitalize(string.toLowerCase().replace(/[_]/g, ' '));

/**
 * A derived list of security classification options from classifiers list, including an empty string option and value
 * @type {Array<ISecurityClassificationOption>}
 */
const securityClassificationDropdownOptions: Array<ISecurityClassificationOption> = [
  '',
  ...classifiers.sort()
].map((value: '' | Classification) => ({
  value,
  label: value ? formatAsCapitalizedStringWithSpaces(value) : '...'
}));

/**
 * Checks if the identifierType is a mixed Id
 * @param {string} identifierType
 * @return {boolean}
 */
const isMixedId = (identifierType: string) => identifierType === fieldIdentifierTypes.generic.value;
/**
 * Checks if the identifierType is a custom Id
 * @param {string} identifierType
  * @return {boolean}
 */
const isCustomId = (identifierType: string) => identifierType === fieldIdentifierTypes.custom.value;

/**
 * Checks if an identifierType has a predefined/immutable value for the field format, i.e. should not be changed by
 * the end user
 * @param {string} identifierType the identifierType to check against
 * @return {boolean}
 */
const hasPredefinedFieldFormat = (identifierType: string) => {
  return isMixedId(identifierType) || isCustomId(identifierType);
};

/**
 * Gets the default logical type for an identifier type
 * @param {string} identifierType
 * @return {string | void}
 */
const getDefaultLogicalType = (identifierType: string): string | void => {
  if (isMixedId(identifierType)) {
    return 'URN';
  }
};

/**
 * Returns a list of logicalType mappings for displaying its value and a label by logicalType
 * @template T IdLogicalType | NonIdLogicalType
 * @template K 'id' | 'generic'
 * @param {K} logicalType
 * @returns {(Array<{ value: T; label: string }>)}
 */
const logicalTypeValueLabel = <T extends IdLogicalType | NonIdLogicalType, K extends 'id' | 'generic'>(
  logicalType: K
) => {
  const logicalTypes: Array<IdLogicalType | NonIdLogicalType> = {
    id: idLogicalTypes,
    generic: genericLogicalTypes
  }[logicalType];

  return logicalTypes.map((value: T) => {
    let label: string;

    // guard checks that if the logical type string is generic, then the value union can be assumed to be
    // a NonIdLogicalType, otherwise it is an id /custom logicalType
    if (logicalType === 'generic') {
      label = nonIdFieldLogicalTypes[<NonIdLogicalType>value].displayAs;
    } else {
      label = value.replace(/_/g, ' ').replace(/([A-Z]{3,})/g, value => Ember.String.capitalize(value.toLowerCase()));
    }

    return {
      value,
      label
    };
  });
};

/**
 * Map logicalTypes to options consumable by DOM
 * @returns {Array<IFieldFormatDropdownOption>}
 */
const logicalTypesForIds: Array<IFieldFormatDropdownOption> = logicalTypeValueLabel('id');

/**
 * Map generic logical type to options consumable in DOM
 * @returns {Array<IFieldFormatDropdownOption>}
 */
const logicalTypesForGeneric: Array<IFieldFormatDropdownOption> = logicalTypeValueLabel('generic');

/**
 * Caches a list of fieldIdentifierTypes values
 * @type {Array<ComplianceFieldIdValue>}
 */
const fieldIdentifierTypeValues: Array<ComplianceFieldIdValue> = Object.values(ComplianceFieldIdValue);

/**
 * Retrieves the default security classification for an identifier type, or null if it does not exist
 * @param {Array<IComplianceDataType>} [complianceDataTypes=[]] the list of compliance data types
 * @param {ComplianceFieldIdValue} identifierType the compliance data type id string
 * @returns {(IComplianceDataType['defaultSecurityClassification'] | null)}
 */
const getDefaultSecurityClassification = (
  complianceDataTypes: Array<IComplianceDataType> = [],
  identifierType: ComplianceFieldIdValue
): IComplianceDataType['defaultSecurityClassification'] | null => {
  const complianceDataType = complianceDataTypes.findBy('id', identifierType);

  return complianceDataType ? complianceDataType.defaultSecurityClassification : null;
};

export {
  securityClassificationDropdownOptions,
  formatAsCapitalizedStringWithSpaces,
  fieldIdentifierTypeValues,
  isMixedId,
  isCustomId,
  hasPredefinedFieldFormat,
  logicalTypesForIds,
  logicalTypesForGeneric,
  getDefaultLogicalType,
  lastSeenSuggestionInterval,
  lowQualitySuggestionConfidenceThreshold,
  logicalTypeValueLabel,
  getDefaultSecurityClassification
};
