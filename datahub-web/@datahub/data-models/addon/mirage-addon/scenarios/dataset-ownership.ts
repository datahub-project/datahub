import { Server } from 'ember-cli-mirage';
import { testDatasetOwnershipUrn } from '@datahub/data-models/mirage-addon/test-helpers/datasets/ownership';
import owners from '@datahub/data-models/mirage-addon/fixtures/dataset-ownership';

export default function(server: Server): void {
  server.createList('datasetOwnership', 3);

  server.createList('datasetOwnership', 1, {
    urn: testDatasetOwnershipUrn,
    owners
  });
}
