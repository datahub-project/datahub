import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Helper | render-links-as-anchor-tags', function(hooks): void {
  setupRenderingTest(hooks);

  test('it correctly detects links and renders the correct markdown', async function(assert): Promise<void> {
    this.set('inputValue', 'https://linkedin.com');

    await render(hbs`{{render-links-as-anchor-tags inputValue}}`);

    assert.dom('a').exists();

    this.set('inputValue', 'Not a link');

    await render(hbs`{{render-links-as-anchor-tags inputValue}}`);

    assert.dom('a').doesNotExist();
  });
});
