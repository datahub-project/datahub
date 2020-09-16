import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Helper | ms-time-as-unix', function(hooks): void {
  setupRenderingTest(hooks);

  test('it works as expected', async function(assert): Promise<void> {
    this.set('inputValue', '12345678');
    await render(hbs`{{ms-time-as-unix inputValue}}`);
    assert.equal(this.element.textContent?.trim(), '12346', 'works for string inputs');

    this.set('inputValue', 12345678);
    await render(hbs`{{ms-time-as-unix inputValue}}`);
    assert.equal(this.element.textContent?.trim(), '12346', 'works for numerical inputs');
  });
});
