import { Server } from 'ember-cli-mirage';
import { getApiRoot, ApiVersion } from '@datahub/utils/api/shared';
import { ownershipEndpoint } from '@datahub/data-models/api/dataset/ownership';
import { getDatasetOwnership } from '@datahub/data-models/mirage-addon/test-helpers/datasets/ownership';
import { getEntity } from '@datahub/data-models/mirage-addon/test-helpers/get-entity';
import { getDatasetSchema } from '@datahub/data-models/mirage-addon/test-helpers/datasets/schema';
/**
 * Shareable mirage/config for dependent modules
 * @param {Server} server the passed in Mirage server instance in the  calling test
 */
export const setup = (server: Server): void => {
  server.namespace = getApiRoot(ApiVersion.v2);

  server.get('/:entityType/:identifier', getEntity);
  server.get(`datasets/:urn/${ownershipEndpoint}`, getDatasetOwnership);
  server.get('/datasets/:urn/schema', getDatasetSchema);
};
