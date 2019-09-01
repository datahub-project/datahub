import { module, test } from 'qunit';
import { arrayToString } from '@datahub/utils/array/array-to-string';

module('Unit | Utility | string', function() {
  test('arrayToString works as expectted', function(assert) {
    assert.equal(arrayToString(['1']), '1');
    assert.equal(arrayToString(['1', '2']), '1 or 2');
    assert.equal(arrayToString(['1', '2', '3']), '1, 2 or 3');
  });
});
