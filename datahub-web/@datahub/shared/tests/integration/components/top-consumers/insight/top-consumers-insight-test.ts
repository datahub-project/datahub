import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { MirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage/mirage-tests';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import topConsumersScenario from '@datahub/shared/mirage-addon/scenarios/top-consumers';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';
import { stubService } from '@datahub/utils/test-helpers/stub-service';

module('Integration | Component | top-consumers/insight/top-consumers-insight', function(hooks) {
  setupRenderingTest(hooks);
  setupMirage(hooks);

  test('It renders top consumers insight for an entity', async function(this: MirageTestContext, assert): Promise<
    void
  > {
    stubService('configurator', {
      getConfig(): boolean {
        return true;
      }
    });

    topConsumersScenario(this.server);

    const entity = new DatasetEntity('urn:li:dataset:(urn:li:dataPlatform:hive,tracking.pageviewevent,EI)');
    const options = {
      component: 'top-consumers/insight/insight-strip',
      isOptional: true
    };

    this.setProperties({
      entity,
      options
    });

    await render(hbs`
      <TopConsumers::Insight::TopConsumersInsight
        @entity={{this.entity}}
        @options={{this.options}}
      />`);

    const classSelector = '.top-consumers-insight';

    assert.dom(classSelector).exists();
  });
});
