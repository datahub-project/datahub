import getActorFromUrn from '@datahub/data-models/utils/get-actor-from-urn';
import { module, test } from 'qunit';

module('Unit | Utility | get-actor-from-urn', function() {
  test('it works as intended', function(assert): void {
    const sampleUrn = 'urn:li:corpuser:pikachu';
    let result = getActorFromUrn(sampleUrn);
    assert.ok(result === 'pikachu', 'Response as expected');

    const sampleBadUrn = 'somethingsomethingdarkside';
    result = getActorFromUrn(sampleBadUrn);
    assert.ok(result === sampleBadUrn, 'No change as expected if given bad urn');
  });
});
