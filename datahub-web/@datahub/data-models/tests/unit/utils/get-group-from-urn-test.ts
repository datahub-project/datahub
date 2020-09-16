import { getGridGroupFromUrn } from '@datahub/data-models/utils/get-group-from-urn';
import { module, test } from 'qunit';

module('Unit | Utility | get-group-from-urn', function() {
  test('it works as intended', function(assert): void {
    const sampleUrn = 'urn:li:gridGroup:pikachu';
    let result = getGridGroupFromUrn(sampleUrn);
    assert.ok(result === 'pikachu', 'Response as expected');

    const sampleBadUrn = 'somethingsomethingdarkside';
    result = getGridGroupFromUrn(sampleBadUrn);
    assert.ok(result === sampleBadUrn, 'No change as expected if given bad urn');
  });
});
