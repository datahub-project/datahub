import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, waitUntil, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { urn } from 'wherehows-web/mirage/fixtures/urn';
import sinon from 'sinon';

module('Integration | Component | datasets/containers/dataset ownership', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    this.server = sinon.createFakeServer();
    this.server.respondImmediately = true;
  });

  hooks.afterEach(function() {
    this.server.restore();
  });

  test('it renders', async function(assert) {
    const lookupClass = '.dataset-owner-table__add-owner';
    this.set('urn', urn);
    this.server.respondWith('GET', /\/api\/v2\/datasets.*/, [
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify({})
    ]);
    this.server.respondWith('GET', '/api/v1/owner/types', [
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify([])
    ]);

    await render(hbs`{{datasets/containers/dataset-ownership urn=urn}}`);

    await waitUntil(() => find(lookupClass));
    assert.equal(
      document.querySelector(lookupClass).textContent.trim(),
      'Add an owner',
      'shows dataset authors component'
    );
  });
});
