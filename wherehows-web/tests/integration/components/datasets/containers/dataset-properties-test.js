import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { waitUntil, find } from 'ember-native-dom-helpers';
import { urn } from 'wherehows-web/mirage/fixtures/urn';
import sinon from 'sinon';

module('Integration | Component | datasets/containers/dataset properties', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    this.server = sinon.createFakeServer();
    this.server.respondImmediately = true;
  });

  hooks.afterEach(function() {
    this.server.restore();
  });

  test('it renders', async function(assert) {
    const labelClass = '.dataset-deprecation-toggle__toggle-header__label';
    this.set('urn', urn);
    this.server.respondWith('GET', /\/api\/v2\/datasets.*/, [
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify({})
    ]);

    await render(hbs`{{datasets/containers/dataset-properties urn=urn}}`);

    assert.ok(find(labelClass), 'renders presentation component');
  });
});
