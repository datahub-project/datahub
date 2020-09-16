import { Server } from 'ember-cli-mirage';

export default function(server: Server): void {
  server.createList('entityFeatureConfs', 1);
}
