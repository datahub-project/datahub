import { getJSON, postJSON } from 'wherehows-web/utils/api/fetcher';
import { datasetUrlByUrn } from 'wherehows-web/utils/api/datasets/shared';
import { IDatasetRetention, IGetDatasetRetentionResponse } from 'wherehows-web/typings/api/datasets/retention';
import { notFoundApiError } from 'wherehows-web/utils/api';
import { fleece } from 'wherehows-web/utils/object';
import { encodeUrn } from 'wherehows-web/utils/validators/urn';

/**
 * Constructs the url for a datasets retention policy
 * @param {string} urn the urn for the dataset
 * @return {string}
 */
const datasetRetentionUrlByUrn = (urn: string): string => `${datasetUrlByUrn(encodeUrn(urn))}/retention`;

/**
 * Fetches the list of retention policy for a dataset by urn
 * @param {string} urn urn for the dataset
 * @return {Promise<Array<IDatasetView>>}
 */
const readDatasetRetentionByUrn = async (urn: string): Promise<IGetDatasetRetentionResponse | null> => {
  try {
    return await getJSON<IGetDatasetRetentionResponse>({ url: datasetRetentionUrlByUrn(urn) });
  } catch (e) {
    if (notFoundApiError(e)) {
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
    data: fleece<IDatasetRetention, 'modifiedTime' | 'modifiedBy'>(['modifiedBy', 'modifiedTime'])(retention)
  });

export { readDatasetRetentionByUrn, saveDatasetRetentionByUrn };
