import { getJSON, returnDefaultIfNotFound } from 'wherehows-web/utils/api/fetcher';
import { datasetUrlByUrn } from 'wherehows-web/utils/api/datasets/shared';
import { encodeUrn } from 'wherehows-web/utils/validators/urn';
import { LineageList } from 'wherehows-web/typings/api/datasets/relationships';

/**
 * Constructs the url for a datasets upstreams
 * @param {string} urn the urn for the child dataset
 * @return {string}
 */
const datasetUpstreamUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/upstreams`;

/**
 * Constructs the url for a datasets downstreams
 * @param {string} urn
 * @return {string}
 */
const datasetDownstreamUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/downstreams`;

/**
 * Fetches the list of upstream datasets for a dataset by urn
 * @param {string} urn urn for the child dataset
 * @return {Promise<Array<IDatasetView>>}
 */
const readUpstreamDatasetsByUrn = (urn: string): Promise<LineageList> =>
  returnDefaultIfNotFound(getJSON<LineageList>({ url: datasetUpstreamUrlByUrn(encodeUrn(urn)) }), []);

/**
 * Requests the downstream datasets for the dataset identified by urn
 * @param {string} urn string urn for the dataset
 * @return {Promise<Array<IDatasetView>>}
 */
const readDownstreamDatasetsByUrn = (urn: string): Promise<LineageList> =>
  returnDefaultIfNotFound(getJSON<LineageList>({ url: datasetDownstreamUrlByUrn(encodeUrn(urn)) }), []);

export { readUpstreamDatasetsByUrn, readDownstreamDatasetsByUrn };
