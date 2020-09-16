import { formatAsCapitalizedStringWithSpaces } from 'datahub-web/utils/helpers/string';
import { IComplianceChangeSet, ISecurityClassificationOption } from 'datahub-web/typings/app/dataset-compliance';
import {
  Classification,
  ComplianceFieldIdValue
} from '@datahub/metadata-types/constants/entity/dataset/compliance-field-types';
import { IComplianceDataType } from '@datahub/metadata-types/types/entity/dataset/compliance-data-types';

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
 * Derives the list of security classification options from the list of classifiers and disables options if
 * the containsPii argument is truthy. Includes a disabled placeholder option: Unspecified
 * @param {boolean = false} containsPii flag indicating if the dataset contains Pii
 * @return {Array<ISecurityClassificationOption>}
 */
const getSecurityClassificationDropDownOptions = (containsPii = false): Array<ISecurityClassificationOption> =>
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
const isMixedId = (identifierType: string): boolean => identifierType === ComplianceFieldIdValue.MixedId;
/**
 * Checks if the identifierType is a custom Id
 * @param {string} identifierType
 * @return {boolean}
 */
const isCustomId = (identifierType: string): boolean => identifierType === ComplianceFieldIdValue.CustomId;

/**
 * Caches a list of fieldIdentifierTypes values
 * @type {Array<ComplianceFieldIdValue>}
 */
const fieldIdentifierTypeValues: Array<ComplianceFieldIdValue> = Object.values(ComplianceFieldIdValue) as Array<
  ComplianceFieldIdValue
>;

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
  fieldIdentifierTypeValues,
  isMixedId,
  isCustomId,
  lowQualitySuggestionConfidenceThreshold,
  getDefaultSecurityClassification
};
