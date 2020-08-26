import { DataModelName } from '@datahub/data-models/constants/entity';
import { getJSON } from '@datahub/utils/api/fetcher';
import { entityApiByUrn } from '@datahub/data-models/api/entity';

/**
 * Creates a url to the top consumers endpoint for a specific entity by urn
 * @param {DataModelName} entityType - the type of entity for which we are making this request
 * @param {string} urn - identifier for the specific entity for which we want to construct the url
 */
const topConsumersUrlByEntityAndUrn = (entityType: DataModelName, urn: string): string =>
  `${entityApiByUrn(urn, entityType)}/top-consumers`;

/**
 * Given an entity type and urn, construct a getter that retrieves the top consumers aspect for the entity
 * @param {DataModelName} entityType - the type of entity for which we want to read like information
 * @param {string} urn - the identifier for the entity for which we want to read like information
 */
export const readTopConsumersForEntity = (
  entityType: DataModelName,
  urn: string
): Promise<Com.Linkedin.Common.EntityTopUsage> => {
  return getJSON<Com.Linkedin.Common.EntityTopUsage>({ url: topConsumersUrlByEntityAndUrn(entityType, urn) });
};
