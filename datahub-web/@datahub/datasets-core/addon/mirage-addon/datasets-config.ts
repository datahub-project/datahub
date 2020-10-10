import { getDataPlatforms } from '@datahub/datasets-core/mirage-addon/helpers/platforms';
import { getDatasetOwnership } from '@datahub/datasets-core/mirage-addon/helpers/ownership';
import { Server } from 'ember-cli-mirage';
import { IMirageServer } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';

// TODO: [META-11940] Looks like mirage server types are incompatible but is outside the scope of this
// migration. Should return to clean up
export function datasetsMirageConfig(server: Server | IMirageServer): void {
  server = server as IMirageServer;
  server.namespace = '/api/v2';

  // Temporary solution as we don't need real upstreams at the moment, we just don't wnat mirage to throw
  // any errors
  server.get('/datasets/:urn/upstreams', () => []);

  server.get('/list/platforms', getDataPlatforms);

  server.get(`datasets/:urn/owners`, getDatasetOwnership);
}
