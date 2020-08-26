import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { testDatasetOwnershipUrn } from '@datahub/data-models/mirage-addon/test-helpers/datasets/ownership';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';
import healthScenario from '@datahub/shared/mirage-addon/scenarios/health-metadata';
import { MirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage/mirage-tests';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';
import HealthContainersHealthMetadata from '@datahub/shared/components/health/containers/health-metadata';
import { render } from '@ember/test-helpers';
import { stubService } from '@datahub/utils/test-helpers/stub-service';
import sinon from 'sinon';

module('Integration | Component | health/containers/health-metadata', function(hooks): void {
  setupRenderingTest(hooks);
  setupMirage(hooks);

  hooks.beforeEach(function() {
    stubService('configurator', {
      getConfig() {}
    });
  });

  test('it renders', async function(this: MirageTestContext, assert): Promise<void> {
    assert.expect(5);
    healthScenario(this.server);

    const entity = DatasetEntity.displayName;
    const urn = testDatasetOwnershipUrn;

    this.setProperties({ entity, urn });

    const component = await getRenderedComponent({
      ComponentToRender: HealthContainersHealthMetadata,
      template: hbs`
        <Health::Containers::HealthMetadata @entityName={{this.entity}} @urn={{this.urn}} as |container|>
          <span id="score">Score: {{container.health.score}}</span>
          <span id="validations">Validations: {{container.health.validations.length}}</span>
        </Health::Containers::HealthMetadata>
      `,
      testContext: this,
      componentName: 'health/containers/health-metadata'
    });

    assert.ok(component.health !== null, 'Expected the health to not be null');
    assert.ok(Array.isArray(component.health?.validations), 'Expected validations to be a list');
    assert.ok(typeof component.health?.score === 'number', 'Expected the health score to be a number');
    assert.dom('#score').hasText(/Score\: \d{1,3}/, 'Expected score to be a number');
    assert.dom('#validations').hasText(/Validations\: \d{1,3}/, 'Expected validations to be a number');
  });

  test('invokes notification notify method on error', async function(this: MirageTestContext, assert): Promise<void> {
    const notify = sinon.fake();
    stubService('notifications', {
      notify
    });

    await render(hbs`
      <Health::Containers::HealthMetadata @entityName="sdfdsf" @urn="sdfsf" />
    `);

    assert.ok(notify.calledOnce, 'Expected notify method on notification service to be called');
  });
});
