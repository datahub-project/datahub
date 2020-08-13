import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Helper | split-text', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    this.set('inputValue', '1234');
    await render(hbs`{{split-text inputValue}}`);
    assert.equal((this.element.textContent as string).trim(), '1234');
  });
});
