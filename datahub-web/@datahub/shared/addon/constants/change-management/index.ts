import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

// List of entities that have Change Management enabled as part of Institutional memory
export const changeManagementEnabledEntityList: Array<string> = [DatasetEntity.displayName];

/**
 * Types of optional recipients
 */
export enum RecipientType {
  IndividualRecipient = 'individualRecipient',
  DistributionList = 'distributionList'
}
