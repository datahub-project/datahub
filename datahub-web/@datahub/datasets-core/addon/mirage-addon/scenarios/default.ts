import { IMirageServer } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { Server } from 'ember-cli-mirage';
import { generateDatasetSchemaFields } from '@datahub/data-models/mirage-addon/test-helpers/datasets/schema';

export const fieldNames = [
  'CONTACT_ID[type = long]',
  'DATA_XML_VERSION[type = long]',
  'DATA[type = string]',
  'DELETED_TS[type = long]',
  'GG_MODI_TS[type = long]',
  'GG_STATUS[type = string]',
  'IS_NOTE_MANUALLY_MOD[type = string]',
  'lumos_dropdate',
  'MODIFIED_DATE'
];

// TODO: [META-11940] Looks like mirage server types are incompatible but is outside the scope of this
// migration. Should return to clean up
export default function(server: Server | IMirageServer): void {
  server = server as IMirageServer;
  server.loadFixtures('compliance-data-types');

  server.createList('datasetComplianceAnnotationTag', 9);
  server.createList('datasetComplianceAnnotationTag', 10, 'asSuggestion');

  generateDatasetSchemaFields(fieldNames, (server as unknown) as Server);

  server.createList('datasetPurgePolicy', 1);
  server.createList('dataset', 1);
  server.createList('datasetComplianceInfo', 1);
  server.createList('datasetExportPolicy', 1);
  server.createList('platform', 20);
}
