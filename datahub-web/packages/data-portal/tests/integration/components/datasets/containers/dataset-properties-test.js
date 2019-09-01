import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | datasets/containers/dataset properties', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    const { server } = this;
    const { uri } = server.create('datasetView');
    const labelClass = '.entity-deprecation__toggle-header__label';

    this.setProperties({ urn: uri, deprecated: false });

    await render(hbs`{{datasets/containers/dataset-properties urn=urn deprecated=deprecated}}`);

    assert.ok(find(labelClass), 'renders presentation component');
  });
});
