import {
  IDatasetsGetResponse,
  IDatasetView,
  IDatasetViewGetResponse
} from 'wherehows-web/typings/api/datasets/dataset';
import { getHeaders, getJSON, cacheApi } from '@datahub/utils/api/fetcher';
import { datasetsUrl, datasetUrlByUrn } from 'wherehows-web/utils/api/datasets/shared';
import { ApiVersion } from 'wherehows-web/utils/api';
import { IDatasetSnapshot } from '@datahub/metadata-types/types/metadata/dataset-snapshot';
import { IReadDatasetsOptionBag } from '@datahub/data-models/types/entity/dataset/index';
import { encodeUrn } from '@datahub/utils/validators/urn';
import { datasetUrlRoot } from '@datahub/data-models/api/dataset/dataset';

/**
 * Reads a dataset by urn, in the li format
 * @param {string} urn
 * @returns {Promise<IDatasetView>}
 */
const readDatasetByUrn = async (urn: string = ''): Promise<IDatasetView> => {
  const { dataset } = await getJSON<Required<Pick<IDatasetViewGetResponse, 'dataset'>>>({
    url: datasetUrlByUrn(encodeUrn(urn))
  });
  return dataset;
};

/**
 * Constructs a url to get a dataset urn given a dataset id
 * @param {number | string} id
 * @return {string}
 */
const datasetUrnTranslationUrlByUrn = (id: number | string): string => `${datasetUrlRoot(ApiVersion.v2)}/idtourn/${id}`;

/**
 * Translates a dataset id to a dataset urn, using the endpoint at datasetIdTranslationUrlByUrn()
 * @param {number | string} id
 * @return {Promise<string>}
 */
const datasetIdToUrn = async (id: number | string): Promise<string> => {
  const headers = await getHeaders({ url: datasetUrnTranslationUrlByUrn(id) });
  return headers.get('whUrn') || '';
};

/**
 * Fetches the datasets for a platform, and prefix and returns the list of datasets in the
 * response
 * @param {IReadDatasetsOptionBag} {
 *   platform,
 *   prefix,
 *   start
 * }
 * @returns {Promise<IDatasetsGetResponse>}
 */
const readDatasets = cacheApi(
  ({ platform, prefix, start }: IReadDatasetsOptionBag): Promise<IDatasetsGetResponse> => {
    const url = datasetsUrl({ platform, prefix, start });
    return getJSON<IDatasetsGetResponse>({ url });
  }
);

/**
 * Reads the snapshot of a dataset
 * @param urn the id of the dataset
 */
export const readDatasetSnapshot = (urn: string): Promise<IDatasetSnapshot> =>
  getJSON({ url: `${datasetUrlRoot(ApiVersion.v2)}/${urn}/snapshot` });

export { readDatasets, readDatasetByUrn, datasetIdToUrn };
