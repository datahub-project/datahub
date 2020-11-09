import isPrimitive from 'dummy/utils/lib/is-primitive';
import { module, test } from 'qunit';

module('Unit | Utility | lib/is-primitive', function() {
  test('it works for positive cases', function(assert) {
    const resultA = isPrimitive('Ash Ketchum');
    const resultB = isPrimitive(5);
    const resultC = isPrimitive(null);
    const resultD = isPrimitive(undefined);
    const resultE = isPrimitive(true);

    assert.ok(resultA, 'Works for strings');
    assert.ok(resultB, 'Works for numbers');
    assert.ok(resultC, 'Works for null');
    assert.ok(resultD, 'WOrks for undefined');
    assert.ok(resultE, 'Works for boolean');
  });

  test('it works for negative cases', function(assert) {
    const resultA = isPrimitive(function() {
      return 'Pikachu';
    });
    const resultB = isPrimitive({ hellodarkness: 'my old friend' });

    assert.notOk(resultA, 'Negative on functions');
    assert.notOk(resultB, 'Negative on objects');
  });
});
