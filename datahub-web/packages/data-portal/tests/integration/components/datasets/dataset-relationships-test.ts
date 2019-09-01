import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { nonHdfsUrn } from 'wherehows-web/mirage/fixtures/urn';
import { setMockConfig, resetConfig } from 'wherehows-web/services/configurator';

module('Integration | Component | datasets/dataset-relationships', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    setMockConfig({
      showLineageGraph: true
    });
  });

  hooks.afterEach(function() {
    resetConfig();
  });

  test('component rendering', async function(assert) {
    this.set('urn', nonHdfsUrn);
    this.set('dataset', {
      uri: nonHdfsUrn
    });

    await render(hbs`
      {{#datasets/dataset-relationships urn=urn dataset=dataset}}
      {{/datasets/dataset-relationships}}
    `);

    assert.ok(this.element, 'renders component into DOM');
  });
});
