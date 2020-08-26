import { DatasetOrigins, IDatasetOriginsResponse } from 'datahub-web/typings/api/datasets/origins';
import { getJSON, returnDefaultIfNotFound } from '@datahub/utils/api/fetcher';
import { datasetUrlByUrn } from '@datahub/data-models/api/dataset/dataset';

/**
 * Constructs the url for a dataset's dataorigins endpoint
 * @param {string} urn the urn of the selected dataset
 */
const datasetOriginsUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/dataorigins`;

/**
 * Fetches the list of DatasetOrigins
 * @param {string} urn the dataset urn
 * @returns {Promise<DatasetOrigins>}
 */
export const readDatasetOriginsByUrn = async (urn: string): Promise<DatasetOrigins> => {
  const fallbackDataOriginsResponse: IDatasetOriginsResponse = {
    dataorigins: []
  };

  // If we do not receive a value for dataorigins use the empty list above
  const { dataorigins } = await returnDefaultIfNotFound(
    getJSON<IDatasetOriginsResponse>({ url: datasetOriginsUrlByUrn(urn) }),
    fallbackDataOriginsResponse
  );

  return dataorigins;
};
