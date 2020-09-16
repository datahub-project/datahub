import { IComplianceInfo } from 'datahub-web/typings/api/datasets/compliance';
import { nullify, Nullify, Omit, pick } from 'datahub-web/utils/object';

/**
 * Makes values in IComplianceInfo that are present / have a corollary in IDatasetRetention type null i.e. keys
 * 'complianceType' | 'compliancePurgeNote'
 * and retains other properties
 * @param {IComplianceInfo} complianceInfo
 * @returns {(Omit<IComplianceInfo, 'complianceType' | 'compliancePurgeNote'> &
 *   Nullify<Pick<IComplianceInfo, 'complianceType' | 'compliancePurgeNote'>>)}
 */
const nullifyRetentionFieldsOnComplianceInfo = (
  complianceInfo: IComplianceInfo
): Omit<IComplianceInfo, 'complianceType' | 'compliancePurgeNote'> &
  Nullify<Pick<IComplianceInfo, 'complianceType' | 'compliancePurgeNote'>> => ({
  ...complianceInfo,
  ...nullify(pick(complianceInfo, ['complianceType', 'compliancePurgeNote']))
});

export { nullifyRetentionFieldsOnComplianceInfo };
