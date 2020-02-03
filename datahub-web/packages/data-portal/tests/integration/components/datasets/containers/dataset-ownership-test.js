import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, waitUntil, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { urn } from 'wherehows-web/mirage/fixtures/urn';
import { resetConfig, setMockConfig } from 'wherehows-web/services/configurator';
import { noop } from 'wherehows-web/utils/helpers/functions';
import { startMirage } from 'wherehows-web/initializers/ember-cli-mirage';

module('Integration | Component | datasets/containers/dataset ownership', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    setMockConfig({});
  });

  hooks.afterEach(function() {
    resetConfig();
  });

  test('it renders', async function(assert) {
    const lookupClass = '.dataset-owner-table__add-owner';
    this.setProperties({ urn: urn, setOwnershipRuleChange: noop });
    const mirage = startMirage();

    mirage.get('/api/v2/datasets', () => ({}));
    mirage.get('/api/v1/owners/types/', () => []);

    await render(hbs`{{datasets/containers/dataset-ownership setOwnershipRuleChange=setOwnershipRuleChange urn=urn}}`);

    await waitUntil(() => find(lookupClass));
    assert.equal(
      document.querySelector(lookupClass).textContent.trim(),
      'Add an owner',
      'shows dataset authors component'
    );
    mirage.shutdown();
  });
});
