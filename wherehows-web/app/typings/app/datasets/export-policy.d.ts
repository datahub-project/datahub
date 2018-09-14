import { ExportPolicyKeys } from 'wherehows-web/constants';

/**
 * Each item in the expected dataset
 */
export interface IExportPolicyTable {
  value: boolean | undefined;
  label: string;
  dataType: ExportPolicyKeys;
}
