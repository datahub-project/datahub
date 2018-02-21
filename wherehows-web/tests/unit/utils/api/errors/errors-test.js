import { throwIfApiError } from 'wherehows-web/utils/api/errors/errors';
import { module, test } from 'qunit';

module('Unit | Utility | api/errors/errors');

test('throwIfApiError exists', function(assert) {
  assert.ok(typeof throwIfApiError === 'function', 'throwIfApiError exists as a function');
});

test('throwIfApiError returns a Promise / thennable', function(assert) {
  assert.ok(
    typeof throwIfApiError({ status: 200, ok: true, json: () => Promise.resolve() }).then === 'function',
    'invocation returns a Promise object / thennable'
  );
});
