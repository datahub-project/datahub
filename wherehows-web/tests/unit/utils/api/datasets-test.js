import datasetComplianceFor from 'wherehows-web/utils/api/datasets';
import { module, test } from 'qunit';

module('Unit | Utility | api/datasets');

test('it has expected functions', function(assert) {
  assert.expect(1);
  const result = datasetComplianceFor(0);

  assert.ok(result instanceof Promise, 'datasetComplianceFor is an async function');
});
