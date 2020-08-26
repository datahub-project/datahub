import { module, test } from 'qunit';
import { jsonValuesMatch } from '@datahub/utils/validators/json';

module('Unit | Utility | validators/json', function() {
  test('it returns expected value when testing two json objects', function(assert): void {
    assert.expect(2);
    const testObjectA = { pikachu: 'raichu', charmander: 'charmeleon' };
    const testObjectB = { pikachu: 'raichu', charmander: 'charmeleon' };
    const testObjectC = { pikachu: 'eevee', charmander: 'charmeleon' };

    const stringifiedTestObjectA = JSON.stringify(testObjectA);
    const stringifiedTestObjectB = JSON.stringify(testObjectB);
    const stringifiedTestObjectC = JSON.stringify(testObjectC);

    const resultA = jsonValuesMatch([stringifiedTestObjectA], [stringifiedTestObjectB]);
    assert.ok(resultA);

    try {
      jsonValuesMatch([stringifiedTestObjectA], [stringifiedTestObjectC]);
    } catch (e) {
      assert.ok(e.message.includes('Expected only'), 'jsonValuesMatch throws an expected error without a match');
    }
  });
});
