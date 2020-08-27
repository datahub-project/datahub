import { IComplianceDataType } from '@datahub/metadata-types/types/entity/dataset/compliance-data-types';

/**
 * Describes the interface for a request to the complianceDataType endpoint
 * @export
 * @interface IComplianceDataTypeResponse
 */
export interface IComplianceDataTypeResponse {
  complianceDataTypes?: Array<IComplianceDataType>;
}
