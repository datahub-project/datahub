import { warn } from '@ember/debug';
import {
  IDataset,
  IDatasetGetResponse,
  IDatasetsGetResponse,
  IDatasetView,
  IDatasetViewGetResponse,
  IReadDatasetsOptionBag
} from 'wherehows-web/typings/api/datasets/dataset';
import { getHeaders, getJSON } from 'wherehows-web/utils/api/fetcher';
import {
  datasetsCountUrl,
  datasetsUrl,
  datasetsUrlRoot,
  datasetUrlById,
  datasetUrlByUrn
} from 'wherehows-web/utils/api/datasets/shared';
import { ApiStatus } from 'wherehows-web/utils/api';

// TODO:  DSS-6122 Create and move to Error module
const datasetApiException = 'An error occurred with the dataset api';
const datasetIdException = 'Dataset reference in unexpected format. Expected a urn or dataset id.';

/**
 * Constructs the dataset view endpoint url from the dataset id
 * @param {number} id the dataset id
 */
const datasetViewUrlById = (id: number) => `${datasetUrlById(id)}/view`;

/**
 * Reads the dataset object from the get endpoint for the given dataset id
 * @param {number} id the id of the dataset
 * @return {Promise<IDataset>}
 */
const readDatasetById = async (id: number | string): Promise<IDataset> => {
  id = parseInt(id + '', 10);
  // if id is less than or equal 0, throw illegal dataset error
  if (id <= 0 || !Number.isInteger(id)) {
    throw new TypeError(datasetIdException);
  }

  const { status, dataset, message } = await getJSON<IDatasetGetResponse>({ url: datasetUrlById(id) });
  let errorMessage = message || datasetApiException;

  if (status === ApiStatus.OK && dataset) {
    return dataset;
  }

  throw new Error(errorMessage);
};

/**
 * Reads a dataset by urn, in the li format
 * @param {string} urn
 * @returns {Promise<IDatasetView>}
 */
const readDatasetByUrn = async (urn: string): Promise<IDatasetView> => {
  const { dataset } = await getJSON<Pick<IDatasetViewGetResponse, 'dataset'>>({ url: datasetUrlByUrn(urn) });
  return dataset!;
};

/**
 * Reads the response from the datasetView endpoint for the provided dataset id
 * @param {number} id
 * @returns {Promise<IDatasetView>}
 */
const readDatasetView = async (id: number): Promise<IDatasetView> => {
  const { status, dataset } = await getJSON<IDatasetViewGetResponse>({ url: datasetViewUrlById(id) });

  if (status === ApiStatus.OK && dataset) {
    return dataset;
  }

  throw new Error(datasetApiException);
};

/**
 * Constructs a url to get a dataset id given a dataset urn
 * @param {string} urn
 * @return {string}
 */
const datasetIdTranslationUrlByUrn = (urn: string): string => {
  return `${datasetsUrlRoot('v1')}/urntoid/${encodeURIComponent(urn)}`;
};

/**
 * Translates a dataset urn string to a dataset id, using the endpoint at datasetIdTranslationUrlByUrn()
 * if a dataset id is not found
 * or an exception occurs, the value returned is zero, which is an illegal dataset id
 * and should be treated as an exception.
 * @param {string} urn
 * @return {Promise<number>}
 */
const datasetUrnToId = async (urn: string): Promise<number> => {
  let datasetId = 0;

  try {
    // The headers object is a Header
    const headers = await getHeaders({ url: datasetIdTranslationUrlByUrn(urn) });
    const stringId = headers.get('datasetid');

    // If stringId is not falsey, parse as int and return, otherwise use default
    if (stringId) {
      datasetId = parseInt(stringId, 10);
    }
  } catch (e) {
    warn(`Exception occurred translating datasetUrn: ${e.message}`);
  }

  return datasetId;
};

/**
 * Fetches the datasets for a platform, and prefix and returns the list of datasets in the
 * response
 * @param {IReadDatasetsOptionBag} {
 *   platform,
 *   prefix
 * }
 * @returns {Promise<IDatasetsGetResponse['elements']>}
 */
const readDatasets = async ({
  platform,
  prefix
}: IReadDatasetsOptionBag): Promise<IDatasetsGetResponse['elements']> => {
  const url = datasetsUrl({ platform, prefix });
  const response = await getJSON<IDatasetsGetResponse>({ url });

  return response ? [...response.elements] : [];
};

/**
 * Gets the number of datasets, if provided, using the platform and prefix also
 * @param {Partial<IReadDatasetsOptionBag>} { platform, prefix }
 * @returns {Promise<number>}
 */
const readDatasetsCount = async ({ platform, prefix }: Partial<IReadDatasetsOptionBag>): Promise<number> => {
  const url = datasetsCountUrl({ platform, prefix });
  return await getJSON<number>({ url });
};

export { readDatasetById, datasetUrnToId, readDatasetView, readDatasets, readDatasetsCount, readDatasetByUrn };
