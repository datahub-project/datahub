import { capitalize } from '@ember/string';
import { IComplianceChangeSet } from 'wherehows-web/components/dataset-compliance';
import { ISecurityClassificationOption } from 'wherehows-web/constants/dataset-compliance';
import { Classification, ComplianceFieldIdValue } from 'wherehows-web/constants/datasets/compliance';
import { IComplianceDataType } from 'wherehows-web/typings/api/list/compliance-datatypes';

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
const classifiers = [
  Classification.HighlyConfidential,
  Classification.Confidential,
  Classification.LimitedDistribution,
  Classification.Internal,
  Classification.Public
];

/**
 * Lists the dataset security classification options that are exluded for datasets containing PII
 * @type {Classification[]}
 */
const classifiersExcludedIfPII = [Classification.Internal, Classification.Public];

/**
 * Takes a string, returns a formatted string. Niche , single use case
 * for now, so no need to make into a helper
 * @param {string} string
 */
const formatAsCapitalizedStringWithSpaces = (string: string) => capitalize(string.toLowerCase().replace(/[_]/g, ' '));

/**
 * Derives the list of security classification options from the list of classifiers and disables options if
 * the containsPii argument is truthy. Includes a disabled placeholder option: Unspecified
 * @param {boolean = false} containsPii flag indicating if the dataset contains Pii
 * @return {Array<ISecurityClassificationOption>}
 */
const getSecurityClassificationDropDownOptions = (containsPii: boolean = false): Array<ISecurityClassificationOption> =>
  [null, ...classifiers].map((value: ISecurityClassificationOption['value']) => ({
    value,
    label: value ? formatAsCapitalizedStringWithSpaces(value) : 'Unspecified',
    isDisabled: !value || (containsPii && classifiersExcludedIfPII.includes(value))
  }));

/**
 * Checks if the identifierType is a mixed Id
 * @param {string} identifierType
 * @return {boolean}
 */
const isMixedId = (identifierType: string) => identifierType === ComplianceFieldIdValue.MixedId;
/**
 * Checks if the identifierType is a custom Id
 * @param {string} identifierType
 * @return {boolean}
 */
const isCustomId = (identifierType: string) => identifierType === ComplianceFieldIdValue.CustomId;

/**
 * Caches a list of fieldIdentifierTypes values
 * @type {Array<ComplianceFieldIdValue>}
 */
const fieldIdentifierTypeValues: Array<ComplianceFieldIdValue> = <Array<ComplianceFieldIdValue>>Object.values(
  ComplianceFieldIdValue
);

/**
 * Retrieves the default security classification for an identifier type, or null if it does not exist
 * @param {Array<IComplianceDataType>} [complianceDataTypes=[]] the list of compliance data types
 * @param {ComplianceFieldIdValue} identifierType the compliance data type id string
 * @returns {(IComplianceDataType['defaultSecurityClassification'] | null)}
 */
const getDefaultSecurityClassification = (
  complianceDataTypes: Array<IComplianceDataType> = [],
  identifierType: IComplianceChangeSet['identifierType']
): IComplianceDataType['defaultSecurityClassification'] | null => {
  const complianceDataType = complianceDataTypes.findBy('id', identifierType || '');

  return complianceDataType ? complianceDataType.defaultSecurityClassification : null;
};

export {
  getSecurityClassificationDropDownOptions,
  formatAsCapitalizedStringWithSpaces,
  fieldIdentifierTypeValues,
  isMixedId,
  isCustomId,
  lastSeenSuggestionInterval,
  lowQualitySuggestionConfidenceThreshold,
  getDefaultSecurityClassification
};
