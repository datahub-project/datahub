import { IMirageRequest } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageWherehowsDBs } from 'wherehows-web/typings/ember-cli-mirage';
import { IDatasetSnapshot } from 'wherehows-web/typings/api/datasets/dataset';

const getDatasetSnapshot = (
  { datasetViews, datasetOwnerships }: IMirageWherehowsDBs,
  request: IMirageRequest<{}, { dataset_id: string }>
): IDatasetSnapshot => {
  const datasetView = datasetViews.where({ uri: request.params!.dataset_id }).models[0];
  return {
    Ownership: datasetOwnerships.where({ datasetId: datasetView.name }).models
  };
};

export { getDatasetSnapshot };
