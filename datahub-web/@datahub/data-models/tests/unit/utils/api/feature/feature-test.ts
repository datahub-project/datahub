import { readFeature } from '@datahub/data-models/api/feature/feature';
import { setupTest } from 'ember-qunit';
import { module } from 'qunit';
import test from 'ember-sinon-qunit/test-support/test';
import { setupSinonTest } from '@datahub/data-models/mirage-addon/test-helpers/setup-sinon-test';

module('Unit | Utility | api/feature/feature', function(hooks): void {
  setupTest(hooks);

  test('readFeature() is a thennable', async function(this: SinonTestContext, assert) {
    assert.expect(2);
    const testFeature = { urn: 'urn' };
    const setupValue = setupSinonTest(this);

    const response = readFeature('urn');

    assert.ok(typeof response.then === 'function', 'expected readFeature invocation to return a promise');

    setupValue.request!.respond(200, { 'Content-Type': 'application/json' }, JSON.stringify(testFeature));
    const value = await response;

    assert.deepEqual(value, testFeature, 'expected value to have the shape of a Feature');

    setupValue.requester.restore();
  });
});
