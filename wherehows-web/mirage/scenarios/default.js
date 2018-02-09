const fixtures = [
  'dataset-nodes',
  'metric-metrics',
  'user-entities',
  'compliance-data-types',
  'list-platforms',
  'dataset-acl-users'
];

export default function(server) {
  server.loadFixtures(...fixtures);
  server.create('config');
  server.createList('owner', 6);
  server.createList('dataset', 10);
  server.createList('datasetView', 2);
  server.createList('flow', 10);
  server.createList('column', 2);
  server.createList('comment', 2);
  server.createList('compliance', 2);
  server.createList('depend', 2);
  server.createList('impact', 2);
  server.createList('instance', 2);
  server.createList('ownerType', 2);
  server.createList('reference', 2);
  server.createList('sample', 2);
  server.createList('suggestion', 2);
  server.createList('platform', 2);
  server.createList('version', 2);
}
