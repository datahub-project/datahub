import { getJSON } from '@datahub/utils/api/fetcher';
import { getApiRoot, ApiVersion } from '@datahub/utils/api/shared';
import { encodeUrn } from '@datahub/utils/validators/urn';
import buildUrl from '@datahub/utils/api/build-url';

/**
 * Will generate lineage api url given an entity URN
 * @param urn
 */
const lineageUrl = (urn: string): string => `${getApiRoot(ApiVersion.v2)}/lineage/graph/${encodeUrn(urn)}`;

/**
 * Will return a Lineage Graph for a specific entity
 * @param urn
 * @param limit the max number of upstreams & downstreams levels to be returned
 */
export const readLineage = (urn: string, limit: number): Promise<Com.Linkedin.Metadata.Graph.Graph> => {
  return getJSON<Com.Linkedin.Metadata.Graph.Graph>({ url: buildUrl(lineageUrl(urn), { limit }) });
};
