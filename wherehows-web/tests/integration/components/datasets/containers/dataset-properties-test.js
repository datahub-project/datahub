import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, waitUntil, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { startMirage } from 'wherehows-web/initializers/ember-cli-mirage';

module('Integration | Component | datasets/containers/dataset properties', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    this.server = startMirage();
  });

  hooks.afterEach(function() {
    this.server.shutdown();
  });

  test('it renders', async function(assert) {
    const { server } = this;
    const { uri } = server.create('datasetView');
    const labelClass = '.dataset-deprecation-toggle__toggle-header__label';

    this.setProperties({ urn: uri, deprecated: false });

    await render(hbs`{{datasets/containers/dataset-properties urn=urn deprecated=deprecated}}`);

    assert.ok(find(labelClass), 'renders presentation component');
  });
});
