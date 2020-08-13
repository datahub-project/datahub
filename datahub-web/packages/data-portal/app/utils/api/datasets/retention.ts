import { getJSON, postJSON } from '@datahub/utils/api/fetcher';
import { IDatasetRetention, IGetDatasetRetentionResponse } from 'datahub-web/typings/api/datasets/retention';
import { omit } from 'datahub-web/utils/object';
import { isNotFoundApiError } from '@datahub/utils/api/shared';
import { datasetUrlByUrn } from '@datahub/data-models/api/dataset/dataset';

/**
 * Constructs the url for a datasets retention policy
 * @param {string} urn the urn for the dataset
 * @return {string}
 */
const datasetRetentionUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/retention`;

/**
 * Fetches the list of retention policy for a dataset by urn
 * @param {string} urn urn for the dataset
 * @return {Promise<Array<IDatasetEntity>>}
 */
const readDatasetRetentionByUrn = async (urn: string): Promise<IGetDatasetRetentionResponse | null> => {
  try {
    return await getJSON<IGetDatasetRetentionResponse>({ url: datasetRetentionUrlByUrn(urn) });
  } catch (e) {
    if (isNotFoundApiError(e)) {
      return null;
    }

    throw e;
  }
};

/**
 * Persists the dataset retention policy remotely
 * @param {string} urn the urn of the dataset to save
 * @param {IDatasetRetention} retention the dataset retention policy to update
 * @return {Promise<IDatasetRetention>}
 */
const saveDatasetRetentionByUrn = (urn: string, retention: IDatasetRetention): Promise<IDatasetRetention> =>
  postJSON<IDatasetRetention>({
    url: datasetRetentionUrlByUrn(urn),
    data: omit(retention, ['modifiedBy', 'modifiedTime'])
  });

export { readDatasetRetentionByUrn, saveDatasetRetentionByUrn };
