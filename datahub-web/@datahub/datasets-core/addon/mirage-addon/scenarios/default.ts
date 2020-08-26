import { IMirageServer } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { Server } from 'ember-cli-mirage';

// TODO: [META-11940] Looks like mirage server types are incompatible but is outside the scope of this
// migration. Should return to clean up
export default function(server: Server | IMirageServer): void {
  server = server as IMirageServer;
  server.loadFixtures('compliance-data-types');

  server.createList('datasetComplianceAnnotationTag', 9);
  server.createList('datasetComplianceAnnotationTag', 10, 'asSuggestion');
  server.createList('datasetSchemaColumn', 9);
  server.createList('datasetPurgePolicy', 1);
  server.createList('datasets', 1);
  server.createList('datasetComplianceInfo', 1);
  server.createList('datasetExportPolicy', 1);
  server.createList('platform', 20);
}
