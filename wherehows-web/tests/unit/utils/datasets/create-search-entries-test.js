import { module, skip } from 'qunit';
import { startMirage } from 'wherehows-web/initializers/ember-cli-mirage';
import datasetsCreateSearchEntries from 'wherehows-web/utils/datasets/create-search-entries';
import { testSchemaA } from 'wherehows-web/mirage/fixtures/schema';

module('Unit | Utility | datasets/create search entries', {
  beforeEach() {
    this.server = startMirage();
  },
  afterEach() {
    this.server.shutdown();
  }
});

skip('it works base case', function(assert) {
  const { server } = this;
  const dataset = server.create('dataset', 'forUnitTests');

  const result = datasetsCreateSearchEntries([]);
  assert.ok(!result, 'Returns without error for nothing case');

  datasetsCreateSearchEntries([dataset]);
  assert.equal(dataset.id, 0, 'Sanity check: Created model successfully');
  assert.equal(dataset.schema, testSchemaA.slice(0, 499), 'Partial schema from beginning if no keyword found');
});

skip('it works for keyword cases', function(assert) {
  const { server } = this;
  const dataset = server.create('dataset', 'forUnitTests');

  datasetsCreateSearchEntries([dataset], 'Rebel');
  assert.equal(dataset.id, 0, 'Sanity check: Created model successfully again');
  assert.equal(dataset.schema, testSchemaA.slice(29, 529), 'Partial schema starts from keyword index');
});
