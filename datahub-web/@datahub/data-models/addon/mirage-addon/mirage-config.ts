import { Server } from 'ember-cli-mirage';
import { getApiRoot, ApiVersion } from '@datahub/utils/api/shared';
import { ownershipEndpoint } from '@datahub/data-models/api/dataset/ownership';
import { getDatasetOwnership } from '@datahub/data-models/mirage-addon/test-helpers/datasets/ownership';

/**
 * Shareable mirage/config for dependent modules
 * @param {Server} server the passed in Mirage server instance in the  calling test
 */
export const setup = (server: Server): void => {
  server.namespace = getApiRoot(ApiVersion.v2);

  server.get(`datasets/:urn/${ownershipEndpoint}`, getDatasetOwnership);
};
