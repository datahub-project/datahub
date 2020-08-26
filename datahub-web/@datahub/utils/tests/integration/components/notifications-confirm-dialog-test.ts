import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

const dialogClassName = '.notification-confirm-modal';
module('Integration | Component | notifications-confirm-dialog', function(hooks): void {
  setupRenderingTest(hooks);

  test('Confirmation Dialog rendering', async function(assert): Promise<void> {
    const header = 'Test Header';
    const content = 'Test Content';
    await render(hbs`<NotificationsConfirmDialog />`);

    assert.dom(dialogClassName).exists();
    this.setProperties({ header, content });

    await render(hbs`<NotificationsConfirmDialog as |Dialog|>
      <Dialog.header @header={{this.header}} />
      <Dialog.content @content={{this.content}} />
      <Dialog.footer />
    </NotificationsConfirmDialog>`);

    assert.dom(`${dialogClassName}__heading-text`).hasText(header);
    assert.dom(`${dialogClassName}__content`).hasText(content);
    assert.equal(
      findAll(`${dialogClassName} button`).length,
      2,
      'Expected there to be two buttons for dialog dismissal and confirmation'
    );
  });
});
