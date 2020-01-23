import noop from '@datahub/entity-deprecation/utils/functions/noop';
import { module, test } from 'qunit';

module('Unit | Utility | functions/noop', function(/*hooks*/) {
  test('it exists', function(assert) {
    assert.ok(typeof noop === 'function');
    assert.ok(noop() === undefined, 'Noop function does not return anything');
  });
});
