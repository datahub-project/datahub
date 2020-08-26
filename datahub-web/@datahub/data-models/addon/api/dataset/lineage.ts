import { datasetUrlByUrn } from '@datahub/data-models/api/dataset/dataset';
import { DatasetLineageList } from '@datahub/metadata-types/types/entity/dataset/lineage';
import { getJSON } from '@datahub/utils/api/fetcher';

/**
 * Constructs the url for a datasets upstreams
 * @param urn the urn for the child dataset
 */
const datasetUpstreamUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/upstreams`;

/**
 * Constructs the url for a datasets downstreams
 * @param urn
 */
const datasetDownstreamUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/downstreams`;

/**
 * Fetches the list of upstream datasets for a dataset by urn
 */
export const readUpstreamDatasets = (urn: string): Promise<DatasetLineageList> =>
  getJSON({ url: datasetUpstreamUrlByUrn(urn) });

/**
 * Requests the downstream datasets for the dataset identified by urn
 * @param urn string urn for the dataset
 */
export const readDownstreamDatasets = (urn: string): Promise<DatasetLineageList> =>
  getJSON({ url: datasetDownstreamUrlByUrn(urn) });
