import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

const modalContentClass = '.notification-confirm-modal__content';

module('Integration | Component | notifications/dialog/dialog-content', function(hooks): void {
  setupRenderingTest(hooks);

  test('Content component rendering', async function(assert): Promise<void> {
    const content = 'Dialog Content';
    this.set('content', content);

    await render(hbs`{{notifications/dialog/dialog-content content=this.content}}`);

    assert.dom(modalContentClass).exists();
    assert.dom(modalContentClass).hasText(content);
  });
});
