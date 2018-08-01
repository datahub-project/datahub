import { IComplianceInfo } from 'wherehows-web/typings/api/datasets/compliance';
import { IDatasetRetention } from 'wherehows-web/typings/api/datasets/retention';
import { pick } from 'wherehows-web/utils/object';
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

export { extractRetentionFromComplianceInfo };
