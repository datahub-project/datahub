import HealthProxy from '@datahub/shared/utils/health/health-proxy';
import { module, test } from 'qunit';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';
import { MirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage/mirage-tests';
import { getSerializedMirageModel } from '@datahub/utils/test-helpers/serialize-mirage-model';
import { setupTest } from 'ember-qunit';

module('Unit | Utility | health/health', function(hooks): void {
  setupTest(hooks);
  setupMirage(hooks);

  test('it works', function(this: MirageTestContext, assert): void {
    const { server } = this;
    let healthProxy = new HealthProxy(null);
    assert.ok(Array.isArray(healthProxy.validations), 'Expected validations to be an array on the Health Proxy');
    assert.ok(healthProxy.score === null, 'Expected score to be null when proxy is instantiated with null');

    server.createList('entityHealth', 1);
    const [healthObject] = getSerializedMirageModel('entityHealths', server);
    healthProxy = new HealthProxy({ score: Math.random(), validations: healthObject.validations });

    assert.ok(String(healthProxy.score).match(/\d{1,3}/));
  });
});
