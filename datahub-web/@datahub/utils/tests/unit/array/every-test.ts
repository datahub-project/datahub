import { arrayEvery } from '@datahub/utils/array/every';
import { module, test } from 'qunit';

module('Unit | Utility | array/every', function() {
  test('it specifies whether every element meets given test', function(assert): void {
    const operationalArray = ['pikachu', 'charmander', 'squirtle', 'bulbasaur'];
    const types: Record<string, string> = {
      pikachu: 'electric',
      charmander: 'fire',
      squirtle: 'water',
      bulbasaur: 'grass'
    };
    const natures: Record<string, string> = {
      pikachu: 'hardy',
      charmander: 'timid',
      squirtle: 'timid',
      bulbasaur: 'hardy'
    };

    const predicateForStringCheck = (element: string): boolean => typeof element === 'string';
    const predicateForTypeCheck = (element: string): boolean => types[element] === 'electric';
    const predicateForNatureCheck = (element: string): boolean =>
      natures[element] === 'hardy' || natures[element] === 'timid';

    assert.ok(arrayEvery(predicateForStringCheck)(operationalArray), 'Correctly specifies every array as string');

    const everyTypeCheckForElectric = arrayEvery(predicateForTypeCheck);

    assert.notOk(everyTypeCheckForElectric(operationalArray), 'Correctly asserts all types are not electric');

    const everyNatureCheckForHardyOrTimid = arrayEvery(predicateForNatureCheck);

    assert.ok(
      everyNatureCheckForHardyOrTimid(operationalArray),
      'Correctly reads array and asserts all elements as hardy or timid'
    );
  });
});
