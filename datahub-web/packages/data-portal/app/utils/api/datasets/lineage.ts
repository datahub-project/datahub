import { getJSON, returnDefaultIfNotFound } from '@datahub/utils/api/fetcher';
import { DatasetLineageList } from '@datahub/metadata-types/types/entity/dataset/lineage';
import { datasetUrlByUrn } from '@datahub/data-models/api/dataset/dataset';

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
 * @return {Promise<Array<IDatasetEntity>>}
 */
const readUpstreamDatasetsByUrn = (urn: string): Promise<DatasetLineageList> =>
  returnDefaultIfNotFound(
    getJSON<DatasetLineageList>({ url: datasetUpstreamUrlByUrn(urn) }),
    []
  );

/**
 * Requests the downstream datasets for the dataset identified by urn
 * @param {string} urn string urn for the dataset
 * @return {Promise<Array<IDatasetEntity>>}
 */
const readDownstreamDatasetsByUrn = (urn: string): Promise<DatasetLineageList> =>
  returnDefaultIfNotFound(
    getJSON<DatasetLineageList>({ url: datasetDownstreamUrlByUrn(urn) }),
    []
  );

export { readUpstreamDatasetsByUrn, readDownstreamDatasetsByUrn };
