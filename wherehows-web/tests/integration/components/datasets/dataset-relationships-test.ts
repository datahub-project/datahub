import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { nonHdfsUrn } from 'wherehows-web/mirage/fixtures/urn';

module('Integration | Component | datasets/dataset-relationships', function(hooks) {
  setupRenderingTest(hooks);

  test('component rendering', async function(assert) {
    this.set('urn', nonHdfsUrn);

    await render(hbs`
      {{#datasets/dataset-relationships urn=urn}}
      {{/datasets/dataset-relationships}}
    `);

    assert.ok(this.element, 'renders component into DOM');
  });
});
