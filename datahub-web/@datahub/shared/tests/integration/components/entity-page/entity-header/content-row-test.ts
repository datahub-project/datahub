import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | entity-page/entity-header/content-row', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    await render(hbs`{{entity-page/entity-header/content-row}}`);

    assert.ok(find('.wherehows-entity-header-content-row'), 'it renders the element with the expected class name');
  });
});
