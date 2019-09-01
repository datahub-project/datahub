import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | institutional-memory/wiki/url-list/add-dialog', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    await render(hbs`{{institutional-memory/wiki/url-list/add-dialog}}`);
    assert.ok(this.element, 'Initial render is without errors');
  });
});
