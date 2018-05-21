import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { datasetUrlByUrn } from 'wherehows-web/utils/api/datasets/shared';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { encodeUrn } from 'wherehows-web/utils/validators/urn';

/**
 * Constructs the url for a datasets upstreams
 * @param {string} urn the urn for the child dataset
 * @return {string}
 */
const datasetUpstreamUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/upstreams`;

/**
 * Fetches the list of upstream datasets for a dataset by urn
 * @param {string} urn urn for the child dataset
 * @return {Promise<Array<IDatasetView>>}
 */
const readUpstreamDatasetsByUrn = (urn: string): Promise<Array<IDatasetView>> =>
  getJSON<Array<IDatasetView>>({ url: datasetUpstreamUrlByUrn(encodeUrn(urn)) });

export { readUpstreamDatasetsByUrn };
