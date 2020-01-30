import { ApiVersion, getApiRoot } from '@datahub/utils/api/shared';
import { encodeUrn } from '@datahub/utils/validators/urn';
import { getJSON } from '@datahub/utils/api/fetcher';

/**
 * Generic entity root path
 * @param entityType the entity type that API accepts
 * @param version version of the API, defaulted to V2
 */
export const entityApiRoot = (entityType: string, version: ApiVersion = ApiVersion.v2): string =>
  `${getApiRoot(version)}/${entityType}`;

/**
 * Generic entity read api url
 * @param urn urn for the entity
 * @param entityType the entity type that API accepts
 */
export const entityApiByUrn = (urn: string, entityType: string): string =>
  `${entityApiRoot(entityType)}/${encodeUrn(urn)}`;

/**
 * Generic entity read api call
 * @param urn urn for the entity
 * @param entityType the entity type that API accepts
 */
export const readEntity = <E>(urn: string, entityType: string): Promise<E> =>
  getJSON<E>({ url: entityApiByUrn(urn, entityType) });
