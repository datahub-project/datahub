import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { waitUntil, find } from 'ember-native-dom-helpers';
import { urn } from 'wherehows-web/mirage/fixtures/urn';
import { initialComplianceObjectFactory } from 'wherehows-web/constants';
import sinon from 'sinon';

module('Integration | Component | datasets/containers/dataset compliance', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    this.server = sinon.createFakeServer();
  });

  hooks.afterEach(function() {
    this.server.restore();
  });

  test('it renders', async function(assert) {
    this.set('urn', urn);
    this.server.respondWith('GET', /\/api\/v2\/datasets\/.*/, [
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify({
        complianceInfo: initialComplianceObjectFactory(urn)
      })
    ]);
    this.server.respondWith(/.*\/compliance-data-types/, [
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify([])
    ]);
    this.server.respondWith(/.*\/compliance\/suggestion/, [
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify({})
    ]);
    this.server.respondWith(/.*\/schema/, [
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify({
        schema: {
          schemaless: false,
          columns: [],
          rawSchema: null,
          keySchema: null
        }
      })
    ]);

    await render(hbs`{{datasets/containers/dataset-compliance urn=urn}}`);
    this.server.respond();

    await waitUntil(() => find('.compliance-container'));
    assert.ok(document.querySelector('empty-state'), 'renders the empty state component');
  });
});
