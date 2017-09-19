import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { module, test } from 'qunit';

module('Unit | Utility | api/fetcher');

test('it has a function getJSON', function(assert) {
  assert.ok(typeof getJSON === 'function');
});
