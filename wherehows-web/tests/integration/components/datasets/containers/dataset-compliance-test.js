import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, waitUntil, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { initialComplianceObjectFactory } from 'wherehows-web/constants';

module('Integration | Component | datasets/containers/dataset compliance', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    assert.expect(1);
    const { server } = this;
    const { uri } = server.create('datasetView');

    this.set('urn', uri);

    await render(hbs`{{datasets/containers/dataset-compliance urn=urn}}`);

    await waitUntil(() => find('.compliance-container'));
    assert.ok(document.querySelector('empty-state'), 'renders the empty state component');
  });
});
