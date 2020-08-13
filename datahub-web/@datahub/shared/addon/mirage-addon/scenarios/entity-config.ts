import { Server } from 'ember-cli-mirage';

export default function(server: Server): void {
  server.createList('entityFeatureConfig', 1);
}
