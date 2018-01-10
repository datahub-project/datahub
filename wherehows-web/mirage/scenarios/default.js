const fixtures = ['dataset-nodes', 'metric-metrics', 'user-entities', 'compliance-data-types', 'list-platforms'];

export default function(server) {
  server.loadFixtures(...fixtures);
  server.create('config');
  server.createList('owner', 6);
  server.createList('dataset', 10);
  server.createList('flow', 10);
}
