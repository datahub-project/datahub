import { getJSON } from '@datahub/utils/api/fetcher';
import { encodeUrn } from '@datahub/utils/validators/urn';
import { ApiVersion, getApiRoot } from '@datahub/utils/api/shared';
import { returnDefaultIfNotFound } from '@datahub/utils/api/fetcher';

/**
 * Constructs the entity configs url root endpoint
 * @param {ApiVersion} version the version of the api midtier route
 */
export const entityConfigsUrlRoot = (version: ApiVersion): string => `${getApiRoot(version)}/entity-configs`;

/**
 * Constructs the url that requests entity configs for the provided urn and target feature
 * @param urn the urn of the entity that we are requesting configs
 * @param targetFeature the target feature that is being configured
 */
export const entityConfigsUrlByUrnAndTargetFeature = (urn: string, targetFeature: string): string =>
  `${entityConfigsUrlRoot(ApiVersion.v2)}/${encodeUrn(urn)}?target=${targetFeature}`;

/**
 * Reads the entity configs given an entity urn and a target feature
 * @param urn the urn of the entity that we are requesting configs
 * @param targetFeature the target feature that is being configured
 */
export const readEntityFeatureConfigs = (urn: string, targetFeature: string): Promise<boolean> => {
  // TODO META-11235: Allow for entity feature configs container to batch targets
  // Currently returns a boolean due to midtier implementation for appworx deprecation
  // Should eventually return a Record where the key is the target feature and the value is the config object or null
  return returnDefaultIfNotFound(
    getJSON<boolean>({ url: entityConfigsUrlByUrnAndTargetFeature(urn, targetFeature) }),
    false
  );
};
