import { Factory } from 'ember-cli-mirage';
import { PurgePolicy } from '@datahub/metadata-types/constants/entity/dataset/compliance/purge-policy';
import { Classification } from '@datahub/metadata-types/constants/entity/dataset/compliance-field-types';
import { DatasetClassifiers } from '@datahub/metadata-types/constants/entity/dataset/compliance/classifiers';
import { IComplianceFieldAnnotation } from '@datahub/metadata-types/constants/entity/dataset/compliance-field-annotation';

export default Factory.extend({
  complianceEntities(): Array<IComplianceFieldAnnotation> {
    return [];
  },
  compliancePurgeNote: null,
  complianceType: PurgePolicy.AutoPurge,
  confidentiality: Classification.LimitedDistribution,
  containingPersonalData: true,
  datasetClassification(): Partial<Record<DatasetClassifiers, boolean>> {
    return {
      [DatasetClassifiers.ACCOUNT_STATUS]: false
    };
  },
  datasetId: null,
  datasetUrn: 'urn',
  modifiedBy: 'catran',
  modifiedTime: 1552521600 * 1000,
  fromUpstream: false
});
