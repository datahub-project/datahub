import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import sinon from 'sinon';

const modalFooterBaseClass = '.notification-confirm-modal__footer';

module('Integration | Component | notifications/dialog/dialog-footer', function(hooks): void {
  setupRenderingTest(hooks);

  test('Footer component rendering', async function(assert): Promise<void> {
    await render(hbs`{{notifications/dialog/dialog-footer}}`);

    assert.dom(modalFooterBaseClass).exists();
    assert
      .dom(`${modalFooterBaseClass}__secondary`)
      .doesNotExist('Expected secondary footer element to not be rendered');

    this.set('dismiss', 'Dismiss');

    await render(hbs`{{#notifications/dialog/dialog-footer dismissButtonText=this.dismiss as |footer|}}
        {{footer.dismissButtonText}}
      {{/notifications/dialog/dialog-footer}}`);

    assert.dom(modalFooterBaseClass).hasText('Dismiss');
  });

  test('Footer component primary & secondary actions', async function(assert): Promise<void> {
    const dismissButtonText = 'Dismiss';
    const confirmButtonText = 'OK';
    const onConfirm = sinon.fake();
    const onDismiss = sinon.fake();

    this.setProperties({
      dismissButtonText,
      confirmButtonText,
      onConfirm,
      onDismiss
    });

    await render(hbs`
      {{notifications/dialog/dialog-footer
        dismissButtonText=this.dismissButtonText
        confirmButtonText=this.confirmButtonText
        onConfirm=this.onConfirm
        onDismiss=this.onDismiss
      }}
    `);

    assert.dom(`${modalFooterBaseClass}__confirm`).hasText(confirmButtonText);
    assert.dom(`${modalFooterBaseClass}__dismiss`).hasText(dismissButtonText);

    await click(`${modalFooterBaseClass}__confirm`);
    assert.ok(onConfirm.calledOnce, 'Expected onConfirmAction to be called once when confirm button clicked');

    await click(`${modalFooterBaseClass}__dismiss`);
    assert.ok(onDismiss.calledOnce, 'Expected onDismiss action to be called when dismiss button clicked');
  });

  test('Footer component tertiary action: Dialog Toggle', async function(assert): Promise<void> {
    const toggleText = 'Toggle Dialog';
    const toggleRef = '#notifications-confirmation-toggle-input';
    const onDialogToggle = sinon.fake();

    this.setProperties({ toggleText, onDialogToggle });

    await render(hbs`
    {{notifications/dialog/dialog-footer
      toggleText=this.toggleText
      onDialogToggle=this.onDialogToggle
    }}
    `);

    assert.dom(`${modalFooterBaseClass}__secondary`).hasText(toggleText);
    const toggleElement = find(toggleRef);
    assert.ok(
      toggleElement && String(toggleElement.getAttribute('type')).toLowerCase() === 'checkbox',
      'Expected the dialog toggle element to be a checkbox'
    );
    assert.dom(toggleRef).isNotChecked();

    await click(toggleRef);

    assert.ok(onDialogToggle.calledOnce, 'Expected the dialog toggle action to be called when checkbox is toggled');
  });
});
