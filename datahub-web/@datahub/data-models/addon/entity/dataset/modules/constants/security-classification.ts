import { IComplianceDataType } from '@datahub/metadata-types/types/entity/dataset/compliance-data-types';
import DatasetComplianceAnnotation from '@datahub/data-models/entity/dataset/modules/compliance-annotation';

/**
 * Retrieves the default security classification for an identifier type, or null if it does not exist
 * @param {Array<IComplianceDataType>} [complianceDataTypes=[]] the list of compliance data types
 * @param {ComplianceFieldIdValue} identifierType the compliance data type id string
 * @returns {(IComplianceDataType['defaultSecurityClassification'] | null)}
 */
export const getDefaultSecurityClassification = (
  complianceDataTypes: Array<IComplianceDataType> = [],
  identifierType: DatasetComplianceAnnotation['identifierType']
): IComplianceDataType['defaultSecurityClassification'] | null => {
  const complianceDataType = complianceDataTypes.findBy('id', identifierType || '');

  return complianceDataType ? complianceDataType.defaultSecurityClassification : null;
};
