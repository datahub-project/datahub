import { IComplianceInfo } from 'wherehows-web/typings/api/datasets/compliance';
import { IDatasetRetention } from 'wherehows-web/typings/api/datasets/retention';
import { nullify, Nullify, Omit, pick } from 'wherehows-web/utils/object';
import { isExempt } from 'wherehows-web/constants';

/**
 * Extracts values from an IComplianceInfo instance to create an instance of IDatasetRetention
 * @param {IComplianceInfo} complianceInfo the compliance info object
 * @returns {IDatasetRetention}
 */
const extractRetentionFromComplianceInfo = (complianceInfo: IComplianceInfo): IDatasetRetention => {
  let { datasetUrn, compliancePurgeNote, complianceType } = pick(complianceInfo, [
    'complianceType',
    'compliancePurgeNote',
    'datasetUrn'
  ]);

  return {
    purgeNote: complianceType && isExempt(complianceType) ? compliancePurgeNote : '',
    purgeType: complianceType,
    datasetUrn: datasetUrn!
  };
};

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

export { extractRetentionFromComplianceInfo, nullifyRetentionFieldsOnComplianceInfo };
