export default function(server) {

  const fixtures = [
    'dataset-nodes',
    'metric-metrics',
    'user-entities'
  ];
  server.loadFixtures(...fixtures);
  server.createList('complianceSuggestion', 5);
  server.createList('owner', 6);
  server.createList('dataset', 10);
  server.createList('flow', 10);

}
