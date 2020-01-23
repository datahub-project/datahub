import { IMirageServer } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { getDatasetEntity } from 'dummy/mirage/helpers/get-dataset-entity';
import { getDatasetSchema } from 'dummy/mirage/helpers/schema';
import { getDataPlatforms } from 'dummy/mirage/helpers/platforms';

export default function(this: IMirageServer): void {
  this.namespace = '/api/v2';

  this.get('/datasets/:urn', getDatasetEntity);

  this.get('/datasets/:urn/schema', getDatasetSchema);
  // Temporary solution as we don't need real upstreams at the moment, we just don't wnat mirage to throw
  // any errors
  this.get('/datasets/:urn/upstreams', () => []);

  this.get('/list/platforms', getDataPlatforms);
}
