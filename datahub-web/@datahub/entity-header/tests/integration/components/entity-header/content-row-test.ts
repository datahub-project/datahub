import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | entity-header/content-row', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    await render(hbs`{{entity-header/content-row}}`);

    assert.ok(find('.wherehows-entity-header-content-row'), 'it renders the element with the expected class name');
  });
});
