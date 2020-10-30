import titleize from 'dummy/utils/strings/titleize';
import { module, test } from 'qunit';

module('Unit | Utility | strings/titleize', function() {
  test('it words for basic cases', function(assert) {
    const resultA = titleize('pikachu');
    const expectedA = 'Pikachu';

    assert.equal(resultA, expectedA, 'Titleizes a single word string as expected');

    const resultB = titleize('pikachuThunderShock');
    const expectedB = 'Pikachu Thunder Shock';

    assert.equal(resultB, expectedB, 'Titelizes camel case as expected');

    const resultC = titleize('eevee_quick_attack');
    const expectedC = 'Eevee Quick Attack';

    assert.equal(resultC, expectedC, 'Titleizes underscore case as expected');
  });

  test('it works for defined words cases', function(assert) {
    const resultA = titleize('electricPIKACHUattack', { defineWords: ['PIKACHU'] });
    const expectedA = 'Electric PIKACHU Attack';

    assert.equal(resultA, expectedA, 'Titleizes correctly with a defined word');

    const resultB = titleize('someUMPMETRIC_dataset', { defineWords: ['UMP', 'METRIC'] });
    const expectedB = 'Some UMP METRIC Dataset';

    assert.equal(resultB, expectedB, 'Titleizes correctly for multiple defined words');

    const resultC = titleize('POKEMON', { defineWords: ['POKEMON'] });
    const expectedC = 'POKEMON';

    assert.equal(resultC, expectedC, 'Does not run into error for single defined word case');
  });
});
