import { datasetUrlByUrn } from '@datahub/data-models/api/dataset/dataset';
import { DatasetLineageList } from '@datahub/metadata-types/types/entity/dataset/lineage';
import { getJSON } from '@datahub/utils/api/fetcher';
import { encodeUrn } from '@datahub/utils/validators/urn';

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
export const readUpstreamDatasets = (urn: string): Promise<DatasetLineageList> =>
  getJSON({ url: datasetUpstreamUrlByUrn(encodeUrn(urn)) });

/**
 * Requests the downstream datasets for the dataset identified by urn
 * @param {string} urn string urn for the dataset
 * @return {Promise<Array<IDatasetView>>}
 */
export const readDownstreamDatasets = (urn: string): Promise<DatasetLineageList> =>
  getJSON({ url: datasetDownstreamUrlByUrn(encodeUrn(urn)) });
