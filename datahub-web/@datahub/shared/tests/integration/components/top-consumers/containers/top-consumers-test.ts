import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { MirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage/mirage-tests';
import topConsumersScenario from '@datahub/shared/mirage-addon/scenarios/top-consumers';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';
import TopConsumersContainer from '@datahub/shared/components/top-consumers/containers/top-consumers';

module('Integration | Component | top-consumers/containers/top-consumers', function(hooks): void {
  setupRenderingTest(hooks);
  setupMirage(hooks);

  test('Container correctly retrieves and reifies data', async function(this: MirageTestContext, assert): Promise<
    void
  > {
    topConsumersScenario(this.server);

    this.set('entity', new DatasetEntity('urn:li:dataset:(urn:li:dataPlatform:hive,tracking.pageviewevent,EI)'));

    const topConsumersContainer = await getRenderedComponent({
      ComponentToRender: TopConsumersContainer,
      testContext: this,
      template: hbs`<TopConsumers::Containers::TopConsumers @entity={{this.entity}} />`,
      componentName: 'top-consumers/containers/top-consumers'
    });

    assert.notEqual(
      topConsumersContainer.topUserUrns?.length,
      0,
      'Expects container to have the correct number of top users urns'
    );
    assert.notEqual(
      topConsumersContainer.topGroupUrns?.length,
      0,
      'Expects container to have the correct number of top group urns'
    );
  });
});
