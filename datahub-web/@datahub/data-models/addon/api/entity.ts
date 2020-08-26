import { ApiVersion, getApiRoot } from '@datahub/utils/api/shared';
import { encodeUrn } from '@datahub/utils/validators/urn';
import { getJSON } from '@datahub/utils/api/fetcher';

/**
 * Generic entity root path
 * @param entityEndpoint the URL path segment or endpoint for the specific entity, this may be different from the entity name
 * @param version version of the API, defaulted to V2
 */
export const entityApiRoot = (entityEndpoint: string, version: ApiVersion = ApiVersion.v2): string =>
  `${getApiRoot(version)}/${entityEndpoint}`;

/**
 * Generic entity read api url
 * @param urn urn for the entity
 * @param entityEndpoint the URL path segment or endpoint for the specific entity, this may be different from the entity name
 */
export const entityApiByUrn = (urn: string, entityEndpoint: string): string =>
  `${entityApiRoot(entityEndpoint)}/${encodeUrn(urn)}`;

/**
 * Generic entity read api call
 * @param urn urn for the entity
 * @param entityEndpoint the URL path segment or endpoint for the specific entity, this may be different from the entity name
 */
export const readEntity = <E>(urn: string, entityEndpoint: string): Promise<E> =>
  getJSON<E>({ url: entityApiByUrn(urn, entityEndpoint) });
