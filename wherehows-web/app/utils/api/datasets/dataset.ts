import {
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
  datasetUrlByUrn
} from 'wherehows-web/utils/api/datasets/shared';
import { encodeUrn } from 'wherehows-web/utils/validators/urn';

/**
 * Reads a dataset by urn, in the li format
 * @param {string} urn
 * @returns {Promise<IDatasetView>}
 */
const readDatasetByUrn = async (urn: string = ''): Promise<IDatasetView> => {
  const { dataset } = await getJSON<Pick<IDatasetViewGetResponse, 'dataset'>>({ url: datasetUrlByUrn(encodeUrn(urn)) });
  return dataset!;
};

/**
 * Constructs a url to get a dataset urn given a dataset id
 * @param {number} id
 * @return {string}
 */
const datasetUrnTranslationUrlByUrn = (id: number): string => `${datasetsUrlRoot('v2')}/idtourn/${id}`;

/**
 * Translates a dataset id to a dataset urn, using the endpoint at datasetIdTranslationUrlByUrn()
 * @param {number} id
 * @return {Promise<string>}
 */
const datasetIdToUrn = async (id: number) => {
  const headers = await getHeaders({ url: datasetUrnTranslationUrlByUrn(id) });
  return headers.get('whUrn');
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
const readDatasets = ({ platform, prefix, start }: IReadDatasetsOptionBag): Promise<IDatasetsGetResponse> => {
  const url = datasetsUrl({ platform, prefix, start });
  return getJSON<IDatasetsGetResponse>({ url });
};

/**
 * Gets the number of datasets, if provided, using the platform and prefix also
 * @param {Partial<IReadDatasetsOptionBag>} { platform, prefix }
 * @returns {Promise<number>}
 */
const readDatasetsCount = ({ platform, prefix }: Partial<IReadDatasetsOptionBag>): Promise<number> => {
  const url = datasetsCountUrl({ platform, prefix });
  return getJSON<number>({ url });
};

export { readDatasets, readDatasetsCount, readDatasetByUrn, datasetIdToUrn };
