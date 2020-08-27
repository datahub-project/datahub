import { ExportPolicyKeys } from '@datahub/metadata-types/constants/entity/dataset/compliance-field-types';

/**
 * Each item in the expected dataset
 */
export interface IExportPolicyTable {
  label: string;
  dataType: ExportPolicyKeys;
  value?: boolean;
}
