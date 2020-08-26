import HealthProxy from '@datahub/shared/utils/health/health-proxy';
import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';
import { MirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage/mirage-tests';
import { getSerializedMirageModel } from '@datahub/utils/test-helpers/serialize-mirage-model';

module('Unit | Utility | health/health-proxy', function(hooks): void {
  setupRenderingTest(hooks);
  setupMirage(hooks);

  test('Proxy attributes and behavior', function(this: MirageTestContext, assert) {
    const { server } = this;
    server.createList('entityHealth', 1, {
      validations: [
        {
          score: 1,
          description: '',
          weight: 1,
          tier: 'MINOR',
          validator: 'com.linkedin.metadata.validators.OwnershipValidator'
        }
      ]
    });

    let { score, validations, lastUpdated } = new HealthProxy();
    assert.notOk(score, 'Expected the health score to be null when constructor invoked without arguments');
    assert.ok(Array.isArray(validations), 'Expected validations to still be an array when no arguments are supplied');
    assert.notOk(validations.length, 'Expected validations to still be an array when no arguments are supplied');
    assert.notOk(lastUpdated, 'Expected lastUpdated to not have a value');

    const [health] = getSerializedMirageModel('entityHealths', server);

    const now = Date.now();
    ({ validations, score, lastUpdated } = new HealthProxy({ ...health, created: { actor: 'test', time: now } }));
    const [validation] = validations;

    assert.equal(
      validation.validator,
      'Ownership',
      'Expected the validation validator string to match the OwnershipValidator'
    );

    assert.equal(typeof score, 'string', 'Expected  the numeric score value to be converted to a string');
    assert.equal(lastUpdated, now, `Expected last updated to match the timestamp ${now}`);
  });
});
