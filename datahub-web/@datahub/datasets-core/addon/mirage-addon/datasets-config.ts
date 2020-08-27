import { getDatasetEntity } from '@datahub/datasets-core/mirage-addon/helpers/get-dataset-entity';
import {
  getDatasetCompliance,
  postDatasetCompliance
} from '@datahub/datasets-core/mirage-addon/helpers/compliance/info';
import { getDatasetSchema } from '@datahub/datasets-core/mirage-addon/helpers/schema';
import { getDatasetComplianceSuggestions } from '@datahub/datasets-core/mirage-addon/helpers/compliance/suggestions';
import { getComplianceDataTypes } from '@datahub/datasets-core/mirage-addon/helpers/compliance/data-types';
import {
  getDatasetExportPolicy,
  postDatasetExportPolicy
} from '@datahub/datasets-core/mirage-addon/helpers/compliance/export-policy';
import { getDataPlatforms } from '@datahub/datasets-core/mirage-addon/helpers/platforms';
import { getDatasetOwnership } from '@datahub/datasets-core/mirage-addon/helpers/ownership';
import {
  getDatasetPurgePolicy,
  postDatasetPurgePolicy
} from '@datahub/datasets-core/mirage-addon/helpers/compliance/purge-policy';
import { Server } from 'ember-cli-mirage';
import { IMirageServer } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';

// TODO: [META-11940] Looks like mirage server types are incompatible but is outside the scope of this
// migration. Should return to clean up
export function datasetsMirageConfig(server: Server | IMirageServer): void {
  server = server as IMirageServer;
  server.namespace = '/api/v2';

  server.get('/datasets/:urn', getDatasetEntity);

  server.get('/datasets/:urn/compliance', getDatasetCompliance);
  // TODO: [META-8403] Make sure the post response is sufficient for compliance entities
  server.post('/datasets/:urn/compliance', postDatasetCompliance);

  server.get('/datasets/:urn/compliance/suggestion', getDatasetComplianceSuggestions);

  server.get('/datasets/:urn/schema', getDatasetSchema);

  server.get('/datasets/:urn/exportpolicy', getDatasetExportPolicy);
  server.post('/datasets/:urn/exportpolicy', postDatasetExportPolicy);

  server.get('/datasets/:urn/retention', getDatasetPurgePolicy);
  server.post('/datasets/:urn/retention', postDatasetPurgePolicy);
  // Temporary solution as we don't need real upstreams at the moment, we just don't wnat mirage to throw
  // any errors
  server.get('/datasets/:urn/upstreams', () => []);

  server.get('/list/compliance-data-types', getComplianceDataTypes);

  server.get('/list/platforms', getDataPlatforms);

  server.get(`datasets/:urn/owners`, getDatasetOwnership);
}
