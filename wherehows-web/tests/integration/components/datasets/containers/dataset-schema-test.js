import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { find } from 'ember-native-dom-helpers';
import { urn } from 'wherehows-web/mirage/fixtures/urn';
import sinon from 'sinon';

module('Integration | Component | datasets/containers/dataset schema', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    this.server = sinon.createFakeServer();
  });

  hooks.afterEach(function() {
    this.server.restore();
  });

  test('it renders', async function(assert) {
    this.set('urn', urn);
    await render(hbs`{{datasets/containers/dataset-schema urn=urn}}`);

    assert.ok(find('#json-viewer'), 'renders the dataset schema component');
  });
});
