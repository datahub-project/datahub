import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { settled, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';
import { MirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage/mirage-tests';
import { getSerializedMirageModel } from '@datahub/utils/test-helpers/serialize-mirage-model';
import HealthHealthFactors, { baseClass } from '@datahub/shared/components/health/health-factors';
import sinon from 'sinon';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';

module('Integration | Component | health/health-factors', function(hooks): void {
  setupRenderingTest(hooks);
  setupMirage(hooks);

  test('it renders', async function(this: MirageTestContext, assert): Promise<void> {
    const { server } = this;
    const titleLabelSelector = `.${baseClass}__title`;
    const ownershipValidatorActionSelector = `.${baseClass}__table button`;
    const onHealthFactorAction = sinon.fake();
    server.createList('entityHealth', 1);
    const [health] = getSerializedMirageModel('entityHealths', server);

    this.setProperties({ health, onHealthFactorAction });

    const component = await getRenderedComponent({
      ComponentToRender: HealthHealthFactors,
      template: hbs`<Health::HealthFactors @health={{this.health}} @onHealthFactorAction={{this.onHealthFactorAction}} />`,
      testContext: this,
      componentName: 'health/health-factors'
    });

    assert.dom(titleLabelSelector).hasText(/Overall Health\:/);
    assert.equal(
      document.querySelectorAll('tbody tr').length,
      health.validations.length,
      'Expected the number of rows to match the available validations'
    );

    server.createList('entityHealth', 1, {
      validations: [
        {
          validator: 'com.linkedin.metadata.validators.OwnershipValidator',
          tier: 'MINOR',
          weight: 3,
          description: '',
          score: 1
        }
      ]
    });

    const [, ownershipHealth] = getSerializedMirageModel('entityHealths', server);

    this.set('health', ownershipHealth);

    await settled();
    assert.dom(ownershipValidatorActionSelector).exists();
    assert.dom(ownershipValidatorActionSelector).hasText('View Owners');

    await click(ownershipValidatorActionSelector);

    assert.ok(
      onHealthFactorAction.calledWith(component.healthProxy.validations.firstObject),
      'Expected action to be called with ownership validator'
    );
  });
});
