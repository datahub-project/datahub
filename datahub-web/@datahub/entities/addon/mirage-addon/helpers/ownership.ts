import { IOwnerResponse } from '@datahub/data-models/types/entity/dataset/ownership';
import owners from '@datahub/data-models/mirage-addon/fixtures/dataset-ownership';

export const getDatasetOwnership = (): IOwnerResponse => ({
  owners,
  fromUpstream: false,
  datasetUrn: '',
  lastModified: 0,
  actor: ''
});
