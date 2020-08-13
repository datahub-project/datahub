import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | page-not-found', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    await render(hbs`<PageNotFound />`);

    assert.equal(this.element.textContent && this.element.textContent.trim(), 'Page not found');
  });
});
