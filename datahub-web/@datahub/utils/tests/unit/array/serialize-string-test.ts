import { module, test } from 'qunit';
import { serializeStringArray } from '@datahub/utils/array/serialize-string';

module('Unit | Utility | array/serialize-string', function() {
  test('it serializes the values in an array to a sorted string', function(assert): void {
    const arrayOfStrings = ['charmander', 'squirtle', 'bulbasaur'];
    const expectedResult = 'bulbasaur,charmander,squirtle';

    assert.equal(serializeStringArray(arrayOfStrings), expectedResult, 'Achieves the expected result');
  });
});
