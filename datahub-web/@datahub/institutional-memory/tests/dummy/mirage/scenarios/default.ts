import { IMirageServer } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';

export default function(server: IMirageServer) {
  server.createList('institutionalMemory', 2);
}
