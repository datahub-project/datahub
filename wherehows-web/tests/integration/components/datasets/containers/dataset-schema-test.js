import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { urn } from 'wherehows-web/mirage/fixtures/urn';

module('Integration | Component | datasets/containers/dataset schema', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    this.set('urn', urn);
    await render(hbs`{{datasets/containers/dataset-schema urn=urn}}`);

    assert.ok(find('#json-viewer'), 'renders the dataset schema component');
  });
});
