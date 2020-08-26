import { getJSON, postJSON } from '@datahub/utils/api/fetcher';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { entityApiByUrn } from '@datahub/data-models/api/entity';
import { isNotFoundApiError } from '@datahub/utils/api/shared';

/**
 * Defines the expected response from the Health endpoint for a given entity
 * @interface IHealthResponse
 */
interface IHealthResponse {
  // Endpoint response for the health metadata
  health: Com.Linkedin.Common.Health;
}

/**
 * Constant value for the Health Endpoint
 */
export const healthEndpoint = 'health-v2';

/**
 * Constructs the entity url using provided specific entity url and appending the health endpoint
 * @param {string} entityUrl a partially constructed READ endpoint url for the specific entity with the identifying urn attached
 */
export const entityHealthUrl = (entityUrl: string): string => `${entityUrl}/${healthEndpoint}`;

/**
 * Requests the Health metadata (aspect) for an entity at the provided entityUrl
 * @param {DataModelEntity} Entity the entity to request Health metadata for
 * @param {string} urn the urn of the specific entity to request
 * @param {boolean} [isRecalculation=false] flag indicating that the recalculation endpoint should be used, this also responds with the updated health score
 */
export const getOrRecalculateHealth = async (
  Entity: DataModelEntity,
  urn: string,
  isRecalculation = false
): Promise<Com.Linkedin.Common.Health> => {
  const endpointPath = Entity.renderProps.entityPage?.apiRouteName;

  if (endpointPath) {
    // the READ endpoint url for a specific entity
    const url = entityHealthUrl(entityApiByUrn(urn, endpointPath));
    const request = isRecalculation ? postJSON<IHealthResponse>({ url, data: {} }) : getJSON<IHealthResponse>({ url });

    try {
      return (await request).health;
    } catch (e) {
      // There may be instances where a GET request for the Health Score may not have been calculated, resulting in a 404
      // In such instances we want to force a recalculation to generate the current score
      // Recursively invoke function with recalculation flag as stop case if the response is not found on GET invocation
      if (isNotFoundApiError(e) && !isRecalculation) {
        return await getOrRecalculateHealth(Entity, urn, true);
      }

      throw e;
    }
  }

  throw new Error(`Invalid endpoint: ${endpointPath}`);
};
