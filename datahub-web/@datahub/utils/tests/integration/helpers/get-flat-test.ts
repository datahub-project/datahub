import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Helper | get-flat', function(hooks): void {
  setupRenderingTest(hooks);

  test('it return the expected value', async function(assert): Promise<void> {
    this.set('obj', {
      something: true,
      'something.with.dots': true
    });

    await render(hbs`{{get-flat obj 'something'}}`);
    assert.equal(this.element.textContent && this.element.textContent.trim(), 'true');

    await render(hbs`{{get-flat obj 'something.with.dots'}}`);
    assert.equal(this.element.textContent && this.element.textContent.trim(), 'true', 'handles properties with "."');

    this.set('obj', undefined);
    await render(hbs`{{get-flat obj 'something.with.dots'}}`);
    assert.equal(this.element.textContent && this.element.textContent.trim(), '', 'it does not fail');
  });
});
