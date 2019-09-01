import { IMirageServer } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';

export default function(server: IMirageServer): void {
  server.loadFixtures('compliance-data-types');

  server.createList('datasetComplianceAnnotationTag', 9);
  server.createList('datasetComplianceAnnotationTag', 10, 'asSuggestion');
  server.createList('datasetSchemaColumn', 9);
  server.createList('datasetPurgePolicy', 1);
  server.createList('datasetView', 1);
  server.createList('datasetComplianceInfo', 1);
  server.createList('platform', 20);
}
