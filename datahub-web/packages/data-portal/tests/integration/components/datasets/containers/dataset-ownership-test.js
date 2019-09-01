import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, waitUntil, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { urn } from 'wherehows-web/mirage/fixtures/urn';
import sinon from 'sinon';
import { resetConfig, setMockConfig } from 'wherehows-web/services/configurator';
import { noop } from 'wherehows-web/utils/helpers/functions';

module('Integration | Component | datasets/containers/dataset ownership', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    setMockConfig({});
    this.sinonServer = sinon.createFakeServer();
    this.sinonServer.respondImmediately = true;
  });

  hooks.afterEach(function() {
    this.sinonServer.restore();
    resetConfig();
  });

  test('it renders', async function(assert) {
    const lookupClass = '.dataset-owner-table__add-owner';
    this.setProperties({ urn: urn, setOwnershipRuleChange: noop });
    this.sinonServer.respondWith('GET', /\/api\/v2\/datasets.*/, [
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify({})
    ]);
    this.sinonServer.respondWith('GET', '/api/v1/owner/types', [
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify([])
    ]);

    await render(hbs`{{datasets/containers/dataset-ownership setOwnershipRuleChange=setOwnershipRuleChange urn=urn}}`);

    await waitUntil(() => find(lookupClass));
    assert.equal(
      document.querySelector(lookupClass).textContent.trim(),
      'Add an owner',
      'shows dataset authors component'
    );
  });
});
