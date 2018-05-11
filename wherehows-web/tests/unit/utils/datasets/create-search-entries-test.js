import datasetsCreateSearchEntries from 'wherehows-web/utils/datasets/create-search-entries';
import { module, test } from 'qunit';
import startMirage from 'wherehows-web/tests/helpers/setup-mirage';
import { testSchemaA } from 'wherehows-web/mirage/data/schema';

module('Unit | Utility | datasets/create search entries', {
  beforeEach() {
    startMirage(this.container);
  },
  afterEach() {
    window.server.shutdown();
  }
});

test('it works base case', function(assert) {
  const server = window.server;
  const model = server.createList('dataset', 1, 'forUnitTests');
  const dataset = model[0];

  let result = datasetsCreateSearchEntries([]);
  assert.ok(!result, 'Returns without error for nothing case');

  result = datasetsCreateSearchEntries(model);
  assert.equal(dataset.id, 0, 'Sanity check: Created model successfully');
  assert.equal(dataset.originalSchema, testSchemaA, 'Preserves original schema properly');
  assert.equal(dataset.schema, testSchemaA.slice(0, 499), 'Partial schema from beginning if no keyword found');
});

test('it works for keyword cases', function(assert) {
  const server = window.server;
  const model = server.createList('dataset', 1, 'forUnitTests');
  const dataset = model[0];

  let result = datasetsCreateSearchEntries(model, 'Rebel');
  assert.equal(dataset.id, 0, 'Sanity check: Created model successfully again');
  assert.equal(dataset.schema, testSchemaA.slice(29, 529), 'Partial schema starts from keyword index');
});
