import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';
import TopConsumersModalContainer from '@datahub/shared/components/top-consumers/containers/top-consumers-modal';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';
import { MirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage/mirage-tests';

module('Integration | Component | top-consumers/containers/top-consumers', function(hooks): void {
  setupRenderingTest(hooks);
  setupMirage(hooks);

  test('Container correctly instantiates the proper entities for top consumers', async function(this: MirageTestContext, assert): Promise<
    void
  > {
    const topUserUrns = ['urn:li:corpuser:pikachu', 'urn:li:corpuser:ashketchum'];
    const topGroupUrns = ['testGroupUrn1', 'testGroupUrn2'];
    this.setProperties({
      topUserUrns,
      topGroupUrns
    });

    const topConsumersModalContainer = await getRenderedComponent({
      ComponentToRender: TopConsumersModalContainer,
      testContext: this,
      template: hbs`<TopConsumers::Containers::TopConsumersModal @topUserUrns={{this.topUserUrns}} @topGroupUrns={{this.topGroupUrns}} />`,
      componentName: 'top-consumers/containers/top-consumers-modal'
    });

    assert.equal(
      topConsumersModalContainer.topUsers.length,
      topUserUrns.length,
      'Expects container to instantiate the correct amount of person instances as the people urns provided'
    );
    assert.equal(
      topConsumersModalContainer.topGroups.length,
      topGroupUrns.length,
      'Expects container to instantiate the correct amount of group instances as the group urns provided'
    );
  });
});
