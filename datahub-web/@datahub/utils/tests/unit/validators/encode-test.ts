import { module, test } from 'qunit';
import { encodeDot, decodeDot } from '@datahub/utils/validators/encode';

module('Unit | Utility | validators/json', function() {
  test('encodeDot and decodeDot works as expected', function(assert): void {
    const testCaseA = 'pikachu.thunder';
    const testCaseB = 'pik.chu.thund.er';

    const resultA = encodeDot(testCaseA);
    const resultB = encodeDot(testCaseB);

    assert.equal(resultA, 'pikachu%2Ethunder', 'Works for a single use case');
    assert.equal(resultB, 'pik%2Echu%2Ethund%2Eer', 'Works for multidot use case');

    const resultC = decodeDot(resultA);
    const resultD = decodeDot(resultB);

    assert.equal(resultC, testCaseA, 'decodeDot reverses encodeDot in single use case');
    assert.equal(resultD, testCaseB, 'decodeDot reverses encodeDot in multidot case');
  });
});
