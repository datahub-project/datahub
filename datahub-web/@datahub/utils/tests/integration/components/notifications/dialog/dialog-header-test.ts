import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | notifications/dialog/dialog-header', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    const header = 'Test Header';
    this.set('header', header);
    await render(hbs`{{notifications/dialog/dialog-header header=this.header}}`);

    assert.dom('.notification-confirm-modal__header').exists();
    assert.dom('.notification-confirm-modal__heading-text').hasText(header);
  });
});
