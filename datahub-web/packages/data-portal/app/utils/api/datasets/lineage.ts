import { getJSON, returnDefaultIfNotFound } from '@datahub/utils/api/fetcher';
import { datasetUrlByUrn } from 'wherehows-web/utils/api/datasets/shared';
import { LineageList } from 'wherehows-web/typings/api/datasets/relationships';
import { encodeUrn } from '@datahub/utils/validators/urn';

// TODO: [META-8686] These lineage API items are still in place here as there is some difference
// in the typings and the actual handling that make the two currently incompatible with a simple
// reference change. Refer to @datahub/data-models/api/dataset/lineage for the maintained version
// that these should be migrated to

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
