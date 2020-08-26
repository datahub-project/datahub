import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { modalClass } from '@datahub/shared/components/tab-content-modal';

module('Integration | Component | top-consumers/insight/top-consumers-modal', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    this.setProperties({
      topUserUrns: [],
      topGroupLinkParams: []
    });

    await render(hbs`
      <TopConsumers::Insight::TopConsumersModal
        @topUserUrns={{this.topUserUrns}}
        @topGroupLinkParams={{this.topGroupLinkParams}}
      />`);

    const classSelector = `.${modalClass}`;
    assert.dom(classSelector).exists();
  });
});
