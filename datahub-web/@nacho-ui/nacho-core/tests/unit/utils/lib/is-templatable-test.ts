import isTemplatable from 'dummy/utils/lib/is-templatable';
import { module, test } from 'qunit';

module('Unit | Utility | lib/is-templatable', function() {
  test('it works for positive cases', function(assert) {
    const resultA = isTemplatable('Eevee');
    const resultB = isTemplatable(['Charmander', 'Squirtle', 'Bulbasaur']);
    const resultC = isTemplatable(new String('Gary Oak'));

    assert.ok(resultA, 'Works for basic strings');
    assert.ok(resultB, 'Works for arrays of strings');
    assert.ok(resultC, 'Works for String objects');
  });

  test('it works for negative cases', function(assert) {
    const resultA = isTemplatable(function() {
      return 'Is this the real life?';
    });
    const resultB = isTemplatable({ isThisJust: 'fantasy?' });

    assert.notOk(resultA, 'Negative for functions');
    assert.notOk(resultB, 'Negative for objects');
  });
});
